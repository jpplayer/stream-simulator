package com.hortonworks.streaming.impl.collectors;

import java.util.Properties;

import javax.jms.MapMessage;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.hortonworks.streaming.impl.domain.AbstractEventCollector;
import com.hortonworks.streaming.impl.domain.wellsfargo.WFBEvent;
import com.hortonworks.streaming.impl.messages.DumpStats;

public class KafkaCollector extends AbstractEventCollector {	
	private Producer<String, String> producer;

	public KafkaCollector() {
		super();
		System.err.println("Initializing Kafka");
		Properties props = new Properties();
        props.put("metadata.broker.list", 
        		"se016.ctolab.hortonworks.com:9092,se017.ctolab.hortonworks.com:9092,se018.ctolab.hortonworks.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
		
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
	}

	public KafkaCollector(int maxEvents) {
		super(maxEvents);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		logger.debug(message);

		if (message instanceof DumpStats) {
			logger.info("Processed " + numberOfEventsProcessed + " events");
		} else if(message instanceof WFBEvent) {
			try {
            	WFBEvent event  = (WFBEvent) message;
            	//guid, filename, type, raw
				KeyedMessage<String, String> data = 
						new KeyedMessage<String, String>("input.securities.wfbpoc.hortonworks", build(event));
                producer.send(data);
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
	
	public static String build(WFBEvent event) {
		//Path of least resistance........just delimit a giant string, I'm not sure how to 
		//do things like a single message with mapped key value pairs in kafka
		String delimiter = "::::::";
		return event.getUuid() + delimiter + event.getTemplate().getFileName() + delimiter  + event.getType() + delimiter  + event.getTemplate().getTemplatePayLoad();		
	}

	public static void main(String[] args) {
		Properties props = new Properties();
        props.put("metadata.broker.list", 
        		"se016.ctolab.hortonworks.com:9092,se017.ctolab.hortonworks.com:9092,se018.ctolab.hortonworks.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
		
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("input.securities.wfbpoc.hortonworks", "STRING TEST");
		producer.send(data);
		
	}
}
