package com.shyam.kafka.kafkagenericserdes;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.shyam.kafka.customserializeranddeserializer.Supplier;

public class KafkaProducerTest {

	public static void main(String args[]) {

		Properties props = new Properties();
		String topic = "MySecondTopic";
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("value.serializer", "com.shyam.kafka.kafkagenericserdes.SupplierGenericSerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, Supplier> producer = new KafkaProducer<>(props);
		ProducerRecord<String, Supplier> record = null;
		Supplier supplier = null;
		for (int i = 0; i < 100; i++) {
			supplier = new Supplier(i, "shyam_" + i, new Date());
			record = new ProducerRecord<String, Supplier>(topic, supplier);
			try {
				Thread.currentThread();
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(record);
		}
		producer.close();

	}
}
