package com.shyam.kafka.kafkagenericserdes;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shyam.kafka.customserializeranddeserializer.Supplier;

public class SupplierGenericDeserializer implements Deserializer<Supplier> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		Deserializer.super.configure(configs, isKey);
	}

	@Override
	public Supplier deserialize(String topic, byte[] data) {

		ObjectMapper objMap = new ObjectMapper();
		Supplier supplier = null;
		try {
			supplier = objMap.readValue(data, Supplier.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return supplier;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		Deserializer.super.close();
	}

}
