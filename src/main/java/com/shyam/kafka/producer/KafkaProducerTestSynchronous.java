package com.shyam.kafka.producer;

import java.util.*;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerTestSynchronous {

	public static void main(String[] args) throws Exception {

		String topicName = "MySecondTopic";
		String key = "Key<%s>";
		String value = "Teri Maa ka saaki naaka... <%s>";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
		try {
			RecordMetadata rmd = producer.send(record).get();
			//System.out.println("Synchronous producer completed with success.");
			ProducerRecord<String, String> record1 = null;//new ProducerRecord<String, String>(topicName, key, value);
			for(int i = 0; i < 100; i++) {
				record1 = new ProducerRecord<String, String>(topicName, String.format(key, i), String.format(value, i));
				rmd = producer.send(record).get();
				System.out.println("Message is sent to partition no: " + rmd.partition() + " and offset: " + rmd.offset());
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
