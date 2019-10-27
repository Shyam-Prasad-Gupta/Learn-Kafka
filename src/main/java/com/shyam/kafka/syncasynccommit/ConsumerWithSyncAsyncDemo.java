package com.shyam.kafka.syncasynccommit;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerWithSyncAsyncDemo {
	
	public static void main(String args[]) {
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "hell");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "false");
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
		kafkaConsumer.subscribe(Arrays.asList("MySecondTopic"));
		ConsumerRecords<String, String> conRecs = null;
		try {
			while(true) {
				conRecs = kafkaConsumer.poll(100);
				for(ConsumerRecord<String,String> record : conRecs) {
					System.out.println(record.value());
				}
				kafkaConsumer.commitAsync();
			}
		}catch(Exception ex) {
			kafkaConsumer.commitSync();
			kafkaConsumer.close();
		}
	}

}
