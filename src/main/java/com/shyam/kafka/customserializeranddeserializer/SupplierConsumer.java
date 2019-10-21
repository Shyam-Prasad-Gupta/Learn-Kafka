package com.shyam.kafka.customserializeranddeserializer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SupplierConsumer {
	
	public static void main(String args[]) {
		
		String topicName = "tesTest";
		String topicGroupName = "supplierGroup";
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.shyam.kafka.customserializeranddeserializer.SupplierDeserializer");
		props.put("group.id", topicGroupName);
		
		KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));
		
		while (true){
            ConsumerRecords<String, Supplier> records = consumer.poll(100);
            for (ConsumerRecord<String, Supplier>record : records){;
                System.out.println("Supplier id= " + String.valueOf(record.value().getSupplierId()) + " Supplier  Name = " + record.value().getSupplierName() + " Supplier Start Date = " + record.value().getSupplierStartDate().toString());
            }
        }		
		
	}

}
