package jms.example;

import java.util.Enumeration;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class BrowserFixed {
	
	public static void main(String[] args) throws Exception {
		new BrowserFixed().exhibitProblem();
	}
	
	private void exhibitProblem() throws Exception {
		InitialContext initialContext = null;
		
		try {
			Properties p = new Properties();

			p.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
			p.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
			p.put(Context.PROVIDER_URL, "jnp://10.0.0.124:1099");

			initialContext = new InitialContext(p);

			createMessages(initialContext);

			//kick off consume in another thread that pauses for 5 seconds after consuming the first message
			final InitialContext _initialContext = initialContext; 
			new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						consumeMessages(_initialContext);
					} catch (NamingException | JMSException e) {
						e.printStackTrace();
					}		
				}
				
			}).start();
			
			// make sure the consume has started before we browse
			Thread.sleep(1000);
			browseQueue(initialContext);
			
			//wait until consume has finished and see what is left.
			Thread.sleep(5000);
			browseQueue(initialContext);
			

		} finally {
			if (initialContext != null) {
				initialContext.close();
			}
		}
	}
	
	private void browseQueue(InitialContext initialContext) throws NamingException, JMSException {
		ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
		Connection connection = cf.createConnection();

		try { 
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) initialContext.lookup("queue/DLQ");

			QueueBrowser browser = session.createBrowser(queue);

			System.err.println("Begin Browsing Queue...");
			@SuppressWarnings("rawtypes")
			Enumeration messageEnum = browser.getEnumeration();
			while (messageEnum.hasMoreElements()) {
				TextMessage message = (TextMessage) messageEnum.nextElement();
				System.out.println("Browsing: " + message.getText());
			}
			System.err.println("End of Browsing Queue...");

			browser.close();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
	
	private void consumeMessages(InitialContext initialContext) throws NamingException, JMSException {
		ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
		Connection connection = cf.createConnection();

		try {
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) initialContext.lookup("queue/DLQ");

			MessageConsumer messageConsumer = session.createConsumer(queue);

			connection.start();

			TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
			System.out.println("Received message: " + messageReceived.getText());
			
			try {
				System.out.println("Sleeping for 5 seconds!");
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			messageReceived = (TextMessage) messageConsumer.receive(5000);
			System.out.println("Received message: " + messageReceived.getText());

		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
	
	
	private void createMessages(InitialContext initialContext) throws NamingException, JMSException {
		
		ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
		Connection connection = cf.createConnection();
		
		try {

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) initialContext.lookup("queue/DLQ");
			MessageProducer producer = session.createProducer(queue);

			TextMessage message_1 = session.createTextMessage("this is the 1st message");
			TextMessage message_2 = session.createTextMessage("this is the 2nd message");
			TextMessage message_3 = session.createTextMessage("this is the 3rd message");

			producer.send(message_1);
			producer.send(message_2);
			producer.send(message_3);

		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
}
