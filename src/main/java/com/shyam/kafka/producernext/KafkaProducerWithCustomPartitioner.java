package com.shyam.kafka.producernext;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerWithCustomPartitioner {

	public static void main(String[] args) throws Exception {

		String topicName = "MyFirstTopic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "com.shyam.kafka.producernext.CustomPartitioner");
		props.put("speed.sensor.name", "TSS");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<String, String>(topicName, "SSP" + i, "500" + i));

		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<String, String>(topicName, "TSS", "500" + i));

		producer.close();
		System.out.println("SimpleProducer Completed.");
	}

}
