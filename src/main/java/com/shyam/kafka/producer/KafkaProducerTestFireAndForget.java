package com.shyam.kafka.producer;

import java.util.*;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
public class KafkaProducerTestFireAndForget {
  
   public static void main(String[] args) throws Exception{
           
      String topicName = "SensorTopic1";
	  String key = "Key1";
	  String value = "Hum do hamare:- ";
      
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9094,localhost:9093");
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        
      Producer<String, String> producer = new KafkaProducer <String, String>(props);
	
	  ProducerRecord<String, String> record = null;//
	  //Future<RecordMetadata> rmd = producer.send(record);
      try {
//    	  for(int i = 0; i < 100; i++) 
    	  int i = 0;
    	  while(true){
    		  record = new ProducerRecord<String, String>(topicName,null,value + i++);
    		  producer.send(record);
    		  System.out.println("Key: " + record.key() + "\nValue: " + record.value());
    	  }
      }catch(Exception ex) {
    	  ex.printStackTrace();
      }finally {
    	  producer.close();  
      }
	  
	  System.out.println("SimpleProducer Completed.");
   }
}
