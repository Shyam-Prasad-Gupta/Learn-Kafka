package com.shyam.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerAsynchronous {

	public static void main(String[] args) throws Exception {

		String topicName = "MySecondTopic";
		String key = "Key1";
		String value = null;

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> record = null;
		for (int i = 0; i < 10; i++){
			//(new Thread()).currentThread().sleep(1000);
			value = i + " Message.";
			record = new ProducerRecord<String, String>(topicName, key, value);
			producer.send(record, new MyKafkaProducerCallback());
		}
		producer.close();

		System.out.println("SimpleProducer Completed.");
	}
}
