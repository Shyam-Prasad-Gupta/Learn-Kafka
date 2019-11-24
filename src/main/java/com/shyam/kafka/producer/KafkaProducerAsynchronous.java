package com.shyam.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * This is all about sending records asynchronously, it provides message delivery acknowledge as in synchronous send
 * and throughput as in fire and forget. But this approach has a problem and that there is a limit on the number of records
 * that we can send before receiving the acknowledge although this limit is configurable. 
 * Here we provide an object of class with implements {@link Callback} and implements its {@link onCompletion({@link RecordMetadata}, {@link Exception}} 
 * method which is called once the acknowledge is received by the produce from the receiving server.
 * 
 * @author shyamprasadgupta
 *
 */
public class KafkaProducerAsynchronous {

	public static void main(String[] args) throws Exception {

		String topicName = "MyFirstTopic";
		String key = "Key";
		String value = null;

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> record = null;
		for (int i = 0; i < 1000; i++){
			//(new Thread()).currentThread().sleep(1000);
			value = i + " Message.";
			record = new ProducerRecord<String, String>(topicName, key + (int)Math.ceil(Math.random()*10), value);
			//here we provide the object whose onCompletion method we want to invoked once acknowledge is received
			producer.send(record, new MyKafkaProducerCallback());
		}
		producer.close();

		System.out.println("SimpleProducer Completed.");
	}
}
