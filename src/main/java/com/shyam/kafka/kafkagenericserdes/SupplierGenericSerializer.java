package com.shyam.kafka.kafkagenericserdes;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shyam.kafka.customserializeranddeserializer.Supplier;

public class SupplierGenericSerializer implements Serializer<Supplier> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		Serializer.super.configure(configs, isKey);
	}

	@Override
	public byte[] serialize(String topic, Supplier data) {
		byte[] retVal = null;
		ObjectMapper objMap = new ObjectMapper();
		try {
			retVal = objMap.writeValueAsString(data).getBytes();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return retVal;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		Serializer.super.close();
	}

}
