package com.shyam.kafka.handlingrebalancing;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaRandomConsumer {

	public static void main(String[] args) throws Exception {

		String topicName = "tesTis";
		KafkaConsumer<String, String> consumer = null;

		String groupName = "RG";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "false");

		consumer = new KafkaConsumer<String, String>(props);
		KafkaRebalanceListener rebalanceListner = new KafkaRebalanceListener(consumer);

		consumer.subscribe(Arrays.asList(topicName), rebalanceListner);
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					/*
					 * System.out.println("Topic:"+ record.topic() + " Partition:" +
					 * record.partition() + " Offset:" + record.offset() + " Value:"+
					 * record.value());
					 */
					// Do some processing and save it to Database
					rebalanceListner.addOffset(record.topic(), record.partition(), record.offset());
				}
				// consumer.commitSync(rebalanceListner.getCurrentOffsets());
			}
		} catch (Exception ex) {
			System.out.println("Exception.");
			ex.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
