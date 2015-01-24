package jms.example;

import java.util.Date;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.management.JMSManagementHelper;
import org.hornetq.jms.client.HornetQMessage;

public class Counter {
	
	private static final String IP_ADDRESS = "10.0.0.124"; 

	
	public static void main(String[] args) throws NamingException, JMSException {
		new Counter().count();
	}
	
	
	private void count() throws NamingException, JMSException {
		QueueConnection connection = null;
		InitialContext initialContext = null;

		try {
			Properties p = new Properties();

			p.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
			p.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
			p.put(Context.PROVIDER_URL, "jnp://"+IP_ADDRESS+":11099");

			initialContext = new javax.naming.InitialContext(p);

			QueueConnectionFactory cf = (QueueConnectionFactory) initialContext.lookup("/StandaloneConnectionFactory");
			connection = cf.createQueueConnection();
			connection.start();

			QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

			
			createManyMessages(initialContext, session);
			countMessages(session);

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {

			if (initialContext != null) {
				initialContext.close();
			}
			if (connection != null) {
				connection.close();
			}

		}
	}

	
	private static void createManyMessages(InitialContext initialContext, QueueSession session) 
			throws NamingException, JMSException {
		
		Queue queue = (Queue) initialContext.lookup("queue/DLQ");
		MessageProducer producer = session.createProducer(queue);

		TextMessage message = session.createTextMessage("This is a text message");

		for (int i = 0; i < 1000; i++) {
			message = session.createTextMessage("This is a text message");
			producer.send(message);
			System.out.println("Sent message: " + message.getText() + " "+ new Date());
		}
	}

	
	private static void countMessages(QueueSession session)
			throws JMSException, Exception {
		
		Queue managementQueue = HornetQJMSClient.createQueue("hornetq.management");
		QueueRequestor requestor = new QueueRequestor(session, managementQueue);

		HornetQMessage managementMessage = (HornetQMessage) session.createMessage();

		JMSManagementHelper.putAttribute((javax.jms.Message) managementMessage, "jms.queue.DLQ", "messageCount");

		HornetQMessage reply = (HornetQMessage) requestor.request((javax.jms.Message) managementMessage);

		int messageCount = (Integer) JMSManagementHelper.getResult((javax.jms.Message) reply);
		System.out.println("DLQ contains "+ messageCount + " messages");
	}

}
