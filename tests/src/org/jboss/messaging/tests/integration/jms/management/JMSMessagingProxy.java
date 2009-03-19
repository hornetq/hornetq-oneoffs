/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.jms.management;

import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.management.ObjectName;

import org.jboss.messaging.jms.server.management.impl.JMSManagementHelper;

/**
 * A MBeanUsingCoreMessage
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class JMSMessagingProxy
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ObjectName on;

   private Session session;

   private QueueRequestor requestor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSMessagingProxy(QueueSession session, Queue managementQueue, ObjectName objectName) throws Exception
   {
      this.session = session;

      this.on = objectName;

      this.requestor = new QueueRequestor(session, managementQueue);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   public Object retrieveAttributeValue(String attributeName)
   {
      try
      {
         Message m = session.createMessage();
         JMSManagementHelper.putAttribute(m, on, attributeName);
         ObjectMessage reply = (ObjectMessage)requestor.request(m);
         return reply.getObject();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public Object invokeOperation(String operationName, Object... args) throws Exception
   {
      Message m = session.createMessage();
      JMSManagementHelper.putOperationInvocation(m, on, operationName, args);
      ObjectMessage reply = (ObjectMessage)requestor.request(m);
      if (JMSManagementHelper.hasOperationSucceeded(reply))
      {
         return reply.getObject();
      }
      else
      {
         throw new Exception(JMSManagementHelper.getOperationExceptionMessage(reply));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
