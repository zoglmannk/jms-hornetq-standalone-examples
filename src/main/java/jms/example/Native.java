package jms.example;

import java.util.HashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;

public class Native {

	private static final String IP_ADDRESS = "10.0.0.124";
	private static final String JMS_QUEUE = "jms.queue.DLQ";

	
	public static void main(String[] args) throws HornetQException {
		HashMap map = new HashMap();
		map.put("host", IP_ADDRESS);
		map.put("port", 15445);
		 
	    TransportConfiguration configuration = new TransportConfiguration(NettyConnectorFactory.class.getName(), map);
	    ServerLocator serverLocator = null;
	    ClientSessionFactory factory = null;
	    ClientSession session = null;		
	    
	    try {
	    	  	serverLocator = (ServerLocator) HornetQClient.createServerLocatorWithoutHA(configuration);
	    	  	serverLocator.setConsumerWindowSize(0);
	    	  	
	            factory = serverLocator.createSessionFactory();
	            session = factory.createSession(false, false, false);
	            
	            //clear out any incompatible messages which could be created from the other examples
	            session = factory.createSession(false, false, false);
	            session.start();
	            ClientConsumer consumer = session.createConsumer(JMS_QUEUE);
	            ClientMessage msg = null;
	            do {
	            	msg = consumer.receive(100);
	            	
	            	if(msg != null) {
		            	msg.acknowledge();
	            		System.out.println("cleared out previous message from queue");
	            	}
	            } while(msg != null);
	            
	            // send a message
	            ClientProducer producer = session.createProducer(JMS_QUEUE);
	            System.out.println(producer.isClosed());
	            ClientMessage message = session.createMessage(true);
	            message.getBodyBuffer().writeString("Hello");
	            System.out.println("sending message = " + message.getBodyBuffer().readString());
	            producer.send(message);
	            session.commit();
	            session.close();
	            
	            // read the message
	            session = factory.createSession(false, false, false);
	            session.start();
	            consumer = session.createConsumer(JMS_QUEUE);
	            		
	            ClientMessage msgReceived = consumer.receive(2000);
	            if(msgReceived == null) {
	            	System.err.println("No message received!");
	            } else {
	            	HornetQBuffer buf = msgReceived.getBodyBuffer();
	            	System.out.println("received message = "+ buf.readString());
	            	msgReceived.acknowledge();
	            }
	            session.commit();
	            session.close();    
	            
	    }catch(Exception ex){
	    	
	    	ex.printStackTrace();
	    	
	    } finally {
	    	if (session != null)
	    		session.close();
	    	if (factory != null)
	    		factory.close();

	    }
		
	}

}
