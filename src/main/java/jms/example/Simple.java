package jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.NamingException;

public class Simple {

	private static final String IP_ADDRESS = "10.0.0.124";
	private static final String QUEUE = "queue/DLQ";
	
	private Context ic;
	private ConnectionFactory cf;
	private Connection connection;
	private Queue queue;
	private Session session;

	
	
	public static void main(String[] args) {
		try {
			Simple simple = new Simple();

			simple.getInitialContext();
			simple.connectAndCreateSession();
			for(int i=0; i<100; i++) {
				simple.produceMessage();
			}
			simple.consumeMessage();
			simple.closeConnection();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	private void getInitialContext() throws NamingException {

		java.util.Properties p = new java.util.Properties();

		p.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
		p.put(javax.naming.Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
		p.put(javax.naming.Context.PROVIDER_URL, "jnp://"+IP_ADDRESS+":11099");

		ic = new javax.naming.InitialContext(p);
	}
	
	private void connectAndCreateSession() throws NamingException, JMSException {
		cf = (javax.jms.ConnectionFactory) ic.lookup("/StandaloneConnectionFactory");
		queue = (javax.jms.Queue) ic.lookup(QUEUE);
		connection = cf.createConnection();
		session = connection.createSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
		connection.start();
	}

	private void produceMessage() throws JMSException {
		String msgContent = "Hello to the world of JMS!";
		javax.jms.MessageProducer publisher = session.createProducer(queue);
		javax.jms.TextMessage message = session.createTextMessage(msgContent);
		publisher.send(message);
		System.out.println("Message sent!");
		publisher.close();
	}

	private void consumeMessage() throws JMSException {
		javax.jms.MessageConsumer messageConsumer = session.createConsumer(queue);
		javax.jms.TextMessage messageReceived = (javax.jms.TextMessage) messageConsumer.receive(5000);

		System.out.println("Received message: " + messageReceived.getText());
		messageConsumer.close();
	}

	private void closeConnection() {
		if (session != null) {
			try {
				session.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

	}


	
}
