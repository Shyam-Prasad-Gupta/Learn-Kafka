package com.shyam.kafka.kafkagenericserdes;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.shyam.kafka.customserializeranddeserializer.Supplier;

public class KafkaConsumerTest {

	public static void main(String... args) {

		Properties props = new Properties();
		String[] topics = { "MySecondTopic" };
		String topicGroupName = "dummyGroupName";
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "com.shyam.kafka.kafkagenericserdes.SupplierGenericDeserializer");
		props.setProperty("group.id", topicGroupName);
		
		KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topics));

		ConsumerRecords<String, Supplier> consumerRecords = null;
		while (true) {
			consumerRecords = consumer.poll(100);
			for (ConsumerRecord<String, Supplier> record : consumerRecords) {
				System.out.println("Supplier id= " + String.valueOf(record.value().getSupplierId())
						+ " Supplier  Name = " + record.value().getSupplierName() + " Supplier Start Date = "
						+ record.value().getSupplierStartDate().toString());
			}
		}

	}

}
