package com.shyam.kafka.producer;

import java.util.*;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * This class demonstrate how kafka can ensure 100% your message is received by
 * the consumer by sending back the acknowledge to the producer. This approach
 * is not good at all as it will reduce the through put significantly.
 * 
 * @author shyamprasadgupta
 *
 */
public class KafkaProducerTestSynchronous {

	public static void main(String[] args) throws Exception {

		String topicName = "MySecondTopic";
		String key = "Key<%s>";
		String value = "Topic - " + topicName + " Message:- <%s>";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
		try {
			RecordMetadata rmd = null;//producer.send(record).get();
			// System.out.println("Synchronous producer completed with success.");
			ProducerRecord<String, String> record1 = null;// new ProducerRecord<String, String>(topicName, key, value);
			for (int i = 0; 4 < 100; i++) {
				//giving dynamic key value just to show that with change in key value partion also changes accordingly
				record1 = new ProducerRecord<String, String>(topicName, String.format(key, (int)Math.ceil(Math.random()*10)), String.format(value, i));
				/*
				 * Here actual work happens here we are send the message and in send() message returns a future object with RecordMetaData object
				 * which contains the metadata for the record which the server has acknowledged.
				 */
				rmd = producer.send(record1).get();
				System.out.println(
						"Message is sent to partition no: " + rmd.partition() + " and offset: " + rmd.offset());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("Message is not sent due to some exception.");
		} finally {
			producer.close();
		}
		System.out.println("SimpleProducer Completed.");
	}
}
