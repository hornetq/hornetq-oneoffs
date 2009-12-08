/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.reattach;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import junit.framework.TestSuite;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * A OrderReattachTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class OrderReattachTest extends ServiceTestBase
{

   // Disabled for now... under investigation (Clebert)
   public static TestSuite suite()
   {
      TestSuite suite = new TestSuite();

      return suite;
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   private final Logger log = Logger.getLogger(this.getClass());

   private HornetQServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testOrderOnSendInVM() throws Throwable
   {
      for (int i = 0; i < 500; i++)
      {
         log.info("#" + getName() + " # " + i);
         doTestOrderOnSend(false);
         tearDown();
         setUp();
      }
   }

   public void doTestOrderOnSend(final boolean isNetty) throws Throwable
   {
      server = createServer(false, isNetty);

      server.start();

      ClientSessionFactory sf = createFactory(isNetty);
      sf.setReconnectAttempts(-1);
      sf.setConfirmationWindowSize(100 * 1024 * 1024);
      sf.setBlockOnNonDurableSend(false);
      sf.setBlockOnAcknowledge(false);

      final ClientSession session = sf.createSession(false, true, true);

      final LinkedBlockingDeque<Boolean> failureQueue = new LinkedBlockingDeque<Boolean>();

      final CountDownLatch ready = new CountDownLatch(1);

      Thread failer = new Thread()
      {
         @Override
         public void run()
         {
            ready.countDown();
            while (true)
            {
               try
               {
                  Boolean poll = false;
                  try
                  {
                     poll = failureQueue.poll(60, TimeUnit.SECONDS);
                  }
                  catch (InterruptedException e)
                  {
                     e.printStackTrace();
                     break;
                  }

                  Thread.sleep(1);

                  final RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionInternal)session).getConnection();

                  // True means... fail session
                  if (poll)
                  {
                     conn.fail(new HornetQException(HornetQException.NOT_CONNECTED, "poop"));
                  }
                  else
                  {
                     // false means... finish thread
                     break;
                  }
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         }
      };

      failer.start();

      ready.await();

      try
      {
         int numberOfProducers = 1;

         final CountDownLatch align = new CountDownLatch(numberOfProducers);
         final CountDownLatch flagStart = new CountDownLatch(1);

         final ClientSessionFactory sessionFactory = sf;

         class ThreadProd extends Thread
         {
            Throwable throwable;

            int count;

            public ThreadProd(final int count)
            {
               this.count = count;
            }

            @Override
            public void run()
            {
               try
               {
                  align.countDown();
                  flagStart.await();
                  doSend2(count, sessionFactory, failureQueue);
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  throwable = e;
               }
            }
         }

         ThreadProd prod[] = new ThreadProd[numberOfProducers];
         for (int i = 0; i < prod.length; i++)
         {
            prod[i] = new ThreadProd(i);
            prod[i].start();
         }

         align.await();
         flagStart.countDown();

         for (ThreadProd prodT : prod)
         {
            prodT.join();
            if (prodT.throwable != null)
            {
               throw prodT.throwable;
            }
         }

      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         try
         {
            sf.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         failureQueue.put(false);

         failer.join();
      }

   }

   final SimpleString ADDRESS = new SimpleString("address");

   public void doSend2(final int order, final ClientSessionFactory sf, final LinkedBlockingDeque<Boolean> failureQueue) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 500;

      final int numSessions = 100;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString("sub" + i);

         // failureQueue.push(true);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.createQueue(ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createMessage(HornetQTextMessage.TYPE,
                                                        false,
                                                        0,
                                                        System.currentTimeMillis(),
                                                        (byte)1);

         if (i % 10 == 0)
         {
            // failureQueue.push(true);
         }
         message.putIntProperty(new SimpleString("count"), i);
         producer.send(message);
      }

      for (ClientSession session : sessions)
      {
         session.start();
      }

      class MyHandler implements MessageHandler
      {
         final CountDownLatch latch = new CountDownLatch(1);

         volatile int count;

         public void onMessage(final ClientMessage message)
         {
            if (count == numMessages)
            {
               Assert.fail("Too many messages");
            }

            if (message.getIntProperty("count") != count)
            {
               log.warn("Error on counter", new Exception("error on counter"));
               System.exit(-1);
            }
            Assert.assertEquals(count, message.getObjectProperty(new SimpleString("count")));

            count++;

            if (count % 100 == 0)
            {
               failureQueue.push(true);
            }

            if (count == numMessages)
            {
               latch.countDown();
            }
         }
      }

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler();

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(10000, TimeUnit.MILLISECONDS);

         Assert.assertTrue(ok);
      }

      // failureQueue.push(true);

      sessSend.close();

      for (ClientSession session : sessions)
      {

         // failureQueue.push(true);
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {

         failureQueue.push(true);

         SimpleString subName = new SimpleString("sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null && server.isStarted())
      {
         server.stop();
      }

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}