package com.shyam.kafka.producer;

import java.util.*;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A simple basic kafka produce which will just keep on sending message. Here we
 * don't care about that the message is received. This approach provides the
 * highest throughput.
 * 
 * @author shyamprasadgupta
 *
 */
public class KafkaProducerTestFireAndForget {

	/**
	 * Driver method which is responsible for sending message to a kafka topic.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		String topicName = "MyFirstTopic";//kafka topic name
		String key = "Key1";//message key: it will be used for partition determination by the kafka
		String value ="Topic - " + topicName + " Message:- ";//value or actual message

		//to initialize a kafka producer we need to provide basic info like kafka broker, key and value serializer
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> record = null;
		try {
			//here infinite loop is deliberate just to simulate continuous data flow at produced end
			for (int i = 0; 4 < 100; i++) {
				//producer record is object with basic information
				record = new ProducerRecord<String, String>(topicName, key, value + i);
				//below statement will send the message record to the kafka broker
				producer.send(record);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			producer.close();
		}

		System.out.println("SimpleProducer Completed.");
	}
}
