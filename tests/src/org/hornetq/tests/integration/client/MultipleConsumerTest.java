/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.client;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.postoffice.AddressManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.JMSTestBase;

/**
 * A MultipleConsumerTest
 *
 * @author clebert
 *
 *
 */
public class MultipleConsumerTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   private static final int TIMEOUT_ON_WAIT = 5000;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected boolean usePersistence()
   {
      return true;
   }

   volatile boolean running = true;

   private static final long WAIT_ON_SEND = 0;

   CountDownLatch errorLatch = new CountDownLatch(1);

   AtomicInteger numberOfErrors = new AtomicInteger(0);

   public void error(Throwable e)
   {
      System.err.println("Error at " + Thread.currentThread().getName());
      e.printStackTrace();
      errorLatch.countDown();
   }

   /**
    * @param destinationID
    * @return
    */
   public Topic createSampleTopic(int destinationID)
   {
      return HornetQJMSClient.createTopic(createTopicName(destinationID));
   }

   /**
    * @param destinationID
    * @return
    */
   private String createTopicName(int destinationID)
   {
      return "topic-input" + destinationID;
   }

   /**
    * @param destinationID
    * @return
    */
   public Queue createSampleQueue(int destinationID)
   {
      return HornetQJMSClient.createQueue(createQueueName(destinationID));
   }

   /**
    * @param destinationID
    * @return
    */
   private String createQueueName(int destinationID)
   {
      return "queue-output-" + destinationID;
   }

   public class Counter extends Thread
   {
      public Counter()
      {
         super("Counter-Thread-Simulating-Management");
      }

      public void run()
      {
         try
         {
            AddressManager addr = ((PostOfficeImpl)server.getPostOffice()).getAddressManager();

            LinkedList<org.hornetq.core.server.Queue> queues = new LinkedList<org.hornetq.core.server.Queue>();
            for (Binding binding : addr.getBindings().values())
            {
               if (binding instanceof QueueBinding)
               {
                  queues.add(((QueueBinding)binding).getQueue());
               }
            }

            while (running)
            {
               Thread.sleep(1000);
               for (org.hornetq.core.server.Queue q : queues)
               {
                  System.out.println("Queue " + q +
                                     " has " +
                                     q.getInstantMessageCount() +
                                     " with " +
                                     q.getMessagesAdded() +
                                     " with " +
                                     q.getConsumerCount() +
                                     " consumers");
               }
            }
         }
         catch (Exception e)
         {
            error(e);
         }
      }
   }

   // It will produce to a destination
   public class ProducerThread extends Thread
   {
      Connection conn;

      Session sess;

      Topic topic;

      MessageProducer prod;

      public ProducerThread(Connection conn, int destinationID) throws Exception
      {
         this.conn = conn;
         this.sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         this.topic = createSampleTopic(destinationID);
         this.prod = sess.createProducer(topic);
      }

      public void run()
      {
         try
         {
            while (running)
            {
               BytesMessage msg = sess.createBytesMessage();
               msg.writeBytes(new byte[1024]);
               prod.send(msg);
               sess.commit();
               Thread.sleep(WAIT_ON_SEND);
            }
         }
         catch (Exception e)
         {
            error(e);
         }
      }
   }

   // It will bridge from one subscription and send to a queue
   public class BridgeSubscriberThread extends Thread
   {
      Session session;

      MessageProducer prod;

      MessageConsumer cons;

      Topic topic;

      Queue outputQueue;

      public BridgeSubscriberThread(Connection masterConn, int destinationID) throws Exception
      {
         super("Bridge_destination=" + destinationID);
         topic = createSampleTopic(destinationID);
         outputQueue = createSampleQueue(destinationID);
         session = masterConn.createSession(true, Session.SESSION_TRANSACTED);
         cons = session.createDurableSubscriber(topic, "bridge-on-" + destinationID);

         prod = session.createProducer(outputQueue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      }

      public void run()
      {
         try
         {

            int i = 0;
            while (running)
            {
               Message msg = cons.receive(TIMEOUT_ON_WAIT);

               if (msg == null)
               {
                  System.err.println("couldn't receive a message within TIMEOUT_ON_WAIT miliseconds on " + topic);
                  error(new RuntimeException("Couldn't receive message"));
               }
               else
               {
                  if (i++ % 100 == 0)
                  {
                     System.out.println(Thread.currentThread().getName() + " received " + i);
                  }
                  prod.send(msg);
                  session.commit();
               }
            }
         }
         catch (Exception e)
         {
            error(e);
         }
      }
   }

   // It will read from a destination, and pretend it finished processing it
   public class ProcessorThread extends Thread
   {

      Connection conn;

      Session session;

      MessageConsumer cons;

      Destination dest;

      final long waitOnEachConsume;

      public ProcessorThread(Connection conn,
                             Session sess,
                             Destination dest,
                             MessageConsumer cons,
                             long waitOnEachConsume) throws Exception
      {
         super("Processor on " + dest);
         this.conn = conn;
         this.session = sess;
         this.dest = dest;
         this.cons = cons;
         this.waitOnEachConsume = waitOnEachConsume;
      }

      public void run()
      {
         int i = 0;
         try
         {
            while (running)
            {
               if (waitOnEachConsume != 0)
               {
                  Thread.sleep(waitOnEachConsume);
               }
               Message msg = cons.receive(TIMEOUT_ON_WAIT);

               if (i++ % 100 == 0)
               {
                  System.out.println(Thread.currentThread().getName() + " processed " + i);
               }
               if (msg == null)
               {
                  System.err.println("couldn't receive a message on processor within TIMEOUT_ON_WAIT miliseconds on " + dest);
                  error(new RuntimeException("Couldn't receive message"));
               }
               else
               {
                  session.commit();
               }
            }
         }
         catch (Exception e)
         {
            error(e);
         }
      }
   }

   // This test requires to be manually tested
   // At the end this test is throwing an OME for some issue on the test itself.
   // As long as you see the message "Finished" the test is considered successfull!
   public void _testMultipleConsumers() throws Throwable
   {

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      set.setPageSizeBytes(10 * 1024);
      set.setMaxSizeBytes(100 * 1024);
      server.getAddressSettingsRepository().addMatch("#", set);

      try
      {
         int nDestinations = 100;

         for (int i = 0; i < nDestinations; i++)
         {
            createTopic(createTopicName(i));
            createQueue(createQueueName(i));
         }

         LinkedList<Connection> connections = new LinkedList<Connection>();

         LinkedList<Thread> consumerThreads = new LinkedList<Thread>();

         LinkedList<Thread> producerThreads = new LinkedList<Thread>();

         // start a few simulated external consumers on the topic (1 external subscription)
         for (int i = 0; i < nDestinations; i++)
         {
            Connection conn = cf.createConnection();
            conn.setClientID("external-consumer-" + i);
            conn.start();
            Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
            Topic topic = createSampleTopic(i);
            MessageConsumer cons = sess.createDurableSubscriber(topic, "ex-" + i);
            ProcessorThread proc = new ProcessorThread(conn, sess, topic, cons, 100l);
            consumerThreads.add(proc);

            connections.add(conn);
         }

         // uncomment this to read from the output queues
         for (int i = 0; i < nDestinations; i++)
         {
            Connection conncons = cf.createConnection();
            conncons.setClientID("output-queue" + i);
            conncons.start();
            Session sesscons = conncons.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = createSampleQueue(i);
            MessageConsumer cons = sesscons.createConsumer(queue);
            ProcessorThread proc = new ProcessorThread(conncons, sesscons, queue, cons, 0l);
            consumerThreads.add(proc);
            connections.add(conncons);
         }

         Connection masterConn = cf.createConnection();
         connections.add(masterConn);
         masterConn.setClientID("master-conn");
         masterConn.start();

         // start the bridges itself
         for (int i = 0; i < nDestinations; i++)
         {
            BridgeSubscriberThread subs = new BridgeSubscriberThread(masterConn, i);
            consumerThreads.add(subs);
         }

         // The producers
         for (int i = 0; i < nDestinations; i++)
         {
            Connection prodConn = cf.createConnection();
            ProducerThread prod = new ProducerThread(prodConn, i);
            producerThreads.add(prod);
         }

         for (Thread t : producerThreads)
         {
            t.start();
         }

         // Waiting some time before we start the consumers. To make sure it's paging
         Thread.sleep(20000);

         System.out.println("starting consumers now");

         for (Thread t : consumerThreads)
         {
            t.start();
         }

         Counter managerThread = new Counter();

         managerThread.start();

         errorLatch.await(20, TimeUnit.MINUTES);

         assertEquals(0, numberOfErrors.get());

         running = false;

         for (Thread t : consumerThreads)
         {
            t.join();
         }

         for (Thread t : producerThreads)
         {
            t.join();
         }

         for (Connection conn : connections)
         {
            conn.close();
         }

         managerThread.join();

         System.out.println("Finished!!!!");

      }
      catch (Throwable e)
      {
         e.printStackTrace();
         throw e;
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
