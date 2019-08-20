package com.shyam.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyKafkaProducerCallback implements Callback {

	public void onCompletion(RecordMetadata metadata, Exception exception) {
		/*try {
			(new Thread()).currentThread().sleep(1000);
		}catch(Exception ex) {
			ex.printStackTrace();
		}*/
		if(exception != null) {
			System.out.println("Asynchronous Producer failed with an exception");
		}else {
			System.out.println("Asynchronous producer succeeded.");
		}
	}
}
