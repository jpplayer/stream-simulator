package com.hortonworks.streaming.impl.collectors;

import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPOutputStream;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.net.util.Base64;

import com.hortonworks.streaming.impl.domain.AbstractEventCollector;
import com.hortonworks.streaming.impl.domain.wellsfargo.WFBEvent;
import com.hortonworks.streaming.impl.messages.DumpStats;
import com.hortonworks.streaming.results.utils.ConfigurationUtil;

public class JmsEventCollector extends AbstractEventCollector {
	private ActiveMQConnectionFactory connectionFactory;
	private String user = ActiveMQConnection.DEFAULT_USER;
	private String password = ActiveMQConnection.DEFAULT_PASSWORD;
	private Connection connection = null;
	private Session session = null;
	private Destination destination = null;
	private MessageProducer producer = null;
	private boolean compressencode = true;
	//Just count number of chars before we compress. Forget bytes for now.
	private long compressthreshold = 15000;

	public JmsEventCollector() {
		super();
		try {
			compressencode = Boolean.parseBoolean(ConfigurationUtil.getInstance().getProperty("jms.compressencode"));
			compressthreshold = Long.parseLong(ConfigurationUtil.getInstance().getProperty("jms.compressthreshold"));
			String host = ConfigurationUtil.getInstance().getProperty(
					"jms.host");
			String port = ConfigurationUtil.getInstance().getProperty(
					"jms.port");
			logger.debug("Setting up JMS Event Collector with host: " + host
					+ " and port: " + port);
			connectionFactory = new ActiveMQConnectionFactory(user, password,
					"tcp://" + host + ":" + port);

			//WFB POC - who knows with activemq...so set it at every level
			connectionFactory.setUseCompression(true);
			connectionFactory.setUseAsyncSend(true);
			connectionFactory.getPrefetchPolicy().setAll(15000);
			connectionFactory.setAlwaysSessionAsync(false);

			connection = connectionFactory.createConnection();

			//WFB POC
			((ActiveMQConnection) connection).setUseCompression(true);
			//WFB POC Only - Message assumed delivery - for POC only
			((ActiveMQConnection) connection).setUseAsyncSend(true);
			//WFB POC Only - Massively larger prefetch so we're not throttled by session queueing
			((ActiveMQConnection) connection).getPrefetchPolicy().setAll(15000);
			//WFB POC Only - By default, a Consumer's session will dispatch messages to the consumer in a separate thread. 
			//If auto acknowledge, you can increase throughput by passing messages straight through the Session to the Consumer 
			((ActiveMQConnection) connection).setAlwaysSessionAsync(false);

			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue(ConfigurationUtil.getInstance()
					.getProperty("jms.queue"));
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		} catch (JMSException e) {
			logger.error(e.getMessage(), e);
		}
	}

	private static byte[] compressRecord(String record) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(record.getBytes("UTF-8"));
		gzip.close();
		//return new String(Base64.encodeBase64(out.toByteArray()));
		return out.toByteArray();
	}
	
	public JmsEventCollector(int maxEvents) {
		super(maxEvents);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		logger.debug(message);

		if (message instanceof DumpStats) {
			logger.info("Processed " + numberOfEventsProcessed + " events");
		} else if(message instanceof WFBEvent) {
			try {
				/*TextMessage textMessage = session.createTextMessage(message
						.toString());
				producer.send(textMessage);*/
				WFBEvent event = (WFBEvent) message;
				MapMessage mapMessage = session.createMapMessage();
				mapMessage.setString("guid", event.getUuid().toString());
				mapMessage.setString("filename", event.getTemplate().getFileName());
				String raw = message.toString();
				boolean compressMe = compressencode && raw.length() > compressthreshold;
				//Brittle, but tell the consumer this is compressed
				mapMessage.setString("type", (compressMe ? "COMP_" : "") + event.getType());
				if(compressMe) {
					//if(event.getTemplate().getFileName().contains("7774")) System.err.println(compressRecord(raw));
					mapMessage.setBytes("raw_message",  compressRecord(raw));
				} else {
					mapMessage.setString("raw_message",  raw);					
				}
				mapMessage.setString("effective_date", event.getEffectiveDate());
				mapMessage.setString("termination_date", event.getTerminationDate());
				
				producer.send(mapMessage);
				logger.debug(raw);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		} else {
			logger.info("I don't know how to handle this:" + message.getClass().getName());
		}
		
		numberOfEventsProcessed++;
		if (numberOfEventsProcessed != -1
				&& numberOfEventsProcessed == maxNumberOfEvents) {
			logger.info("Maximum number of messages processed, exiting");
			this.getContext().system().shutdown();
			System.exit(0);
		}
	}
}
