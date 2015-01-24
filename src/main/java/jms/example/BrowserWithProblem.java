package jms.example;

import java.util.Enumeration;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * You should see the following console output, which shows the buffer hiding messages from the
 * browser. 
 
removing all messages from queue before experiment
Received message: this is the 1st message
Sleeping for 5 seconds!
Begin Browsing Queue...
End of Browsing Queue...
Received message: this is the 2nd message
Begin Browsing Queue...
Browsing: this is the 3rd message
End of Browsing Queue...

 *
 */
public class BrowserWithProblem {
	
	private static final String IP_ADDRESS = "10.0.0.124"; 
	
	private static final String QUEUE = "queue/DLQ";
	private static final String CONNECTION_FACTORY = "/StandaloneConnectionFactory";

	public static void main(String[] args) throws Exception {
		new BrowserWithProblem().exhibitProblem();
	}
	
	private void exhibitProblem() throws Exception {
		InitialContext initialContext = null;
		
		try {
			Properties p = new Properties();

			p.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
			p.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
			p.put(Context.PROVIDER_URL, "jnp://"+IP_ADDRESS+":11099");

			initialContext = new InitialContext(p);

			consumeAllMessages(initialContext);
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
		ConnectionFactory cf = (ConnectionFactory) initialContext.lookup(CONNECTION_FACTORY);
		Connection connection = cf.createConnection();

		try { 
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) initialContext.lookup(QUEUE);

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
	
	private void consumeAllMessages(InitialContext initialContext) throws NamingException, JMSException {
		ConnectionFactory cf = (ConnectionFactory) initialContext.lookup(CONNECTION_FACTORY);
		Connection connection = cf.createConnection();

		try {
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) initialContext.lookup(QUEUE);

			MessageConsumer messageConsumer = session.createConsumer(queue);

			connection.start();

			Message msg = null;
			do {
				msg = messageConsumer.receive(1000);
			} while(msg != null);

			System.err.println("removing all messages from queue before experiment");

		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
	
	private void consumeMessages(InitialContext initialContext) throws NamingException, JMSException {
		ConnectionFactory cf = (ConnectionFactory) initialContext.lookup(CONNECTION_FACTORY);
		Connection connection = cf.createConnection();

		try {
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) initialContext.lookup(QUEUE);

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
		
		ConnectionFactory cf = (ConnectionFactory) initialContext.lookup(CONNECTION_FACTORY);
		Connection connection = cf.createConnection();
		
		try {

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = (Queue) initialContext.lookup(QUEUE);
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
