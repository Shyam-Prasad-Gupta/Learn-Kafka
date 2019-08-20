package com.shyam.kafka.customserializeranddeserializer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class SupplierSerializer implements Serializer<Supplier> {

	private String encoding = "UTF8";

	public void configure(Map<String, ?> configs, boolean isKey) {
		Serializer.super.configure(configs, isKey);
	}

	public byte[] serialize(String topic, Supplier data) {

		int sizeOfSupplierName;
		int sizeOfSupplierDate;
		byte[] supplierNameByteArr;
		byte[] supplierDateByteArr;

		try {
			if (data == null) {
				return null;
			} else {
				supplierNameByteArr = data.getSupplierName().getBytes(encoding);
				sizeOfSupplierName = supplierNameByteArr.length;

				supplierDateByteArr = data.getSupplierStartDate().toString().getBytes(encoding);
				sizeOfSupplierDate = supplierDateByteArr.length;

				ByteBuffer buff = ByteBuffer.allocate(4 + 4 + sizeOfSupplierName + 4 + sizeOfSupplierDate);

				buff.putInt(data.getSupplierId());
				buff.putInt(sizeOfSupplierName);
				buff.put(supplierNameByteArr);
				buff.putInt(sizeOfSupplierDate);
				buff.put(supplierDateByteArr);

				return buff.array();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new SerializationException("Error while serializing Supplier to byte[].");
		}
	}

	/*
	 * public byte[] serialize(String topic, Headers headers, Supplier data) {
	 * return Serializer.super.serialize(topic, headers, data); }
	 */

	public void close() {
		Serializer.super.close();
	}

}
