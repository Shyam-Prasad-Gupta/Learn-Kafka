package com.shyam.kafka.producer;

import java.util.*;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
public class KafkaProducerTestFireAndForget {
  
   public static void main(String[] args) throws Exception{
           
      String topicName = "test";
	  String key = "Key1";
	  String value = "Teri Maa ka saaki naaka... ha ha ha lol...";
      
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        
      Producer<String, String> producer = new KafkaProducer <String, String>(props);
	
	  ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key,value);
	  Future<RecordMetadata> rmd = producer.send(record);
      producer.close();
	  
	  System.out.println("SimpleProducer Completed.");
   }
}
