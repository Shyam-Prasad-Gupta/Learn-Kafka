package com.shyam.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyKafkaProducerCallback implements Callback {

	public void onCompletion(RecordMetadata rmd, Exception exception) {
		/*try {
			(new Thread()).currentThread().sleep(1000);
		}catch(Exception ex) {
			ex.printStackTrace();
		}*/
		if(exception == null) {
			System.out.println("Message is sent to partition no: " + rmd.partition() + " and offset: " + rmd.offset());
		}else {
			System.out.println("Asynchronous producer failed.");
		}
	}
}
