package com.shyam.kafka.customserializeranddeserializer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class SupplierDeserializer implements Deserializer<Supplier> {

	private String encoding = "UTF8";

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Deserializer.super.configure(configs, isKey);
	}

	@Override
	public Supplier deserialize(String topic, byte[] data) {

		if (data == null || data.length == 0) {
			return null;
		} else {
			try {
				ByteBuffer buff = ByteBuffer.wrap(data);

				int supplierId = buff.getInt();
				
				int sizeOfSupplierName = buff.getInt();
				byte[] supplierNameBytes = new byte[sizeOfSupplierName];
				buff.get(supplierNameBytes);
				String deserializedSupplierName = new String(supplierNameBytes, encoding);

				int sizeOfSupplierDate = buff.getInt();
				byte[] supplierDateBytes = new byte[sizeOfSupplierDate];
				buff.get(supplierDateBytes);
				String dateString = new String(supplierDateBytes, encoding);
				DateFormat df = new SimpleDateFormat("EEE MMM ddHH:mm:ss Z yyyy");
				Date supplierDate = df.parse(dateString);

				return new Supplier(supplierId, deserializedSupplierName, supplierDate);

			} catch (Exception ex) {
				ex.printStackTrace();
				throw new SerializationException("Exception in deserializing the Supplier bytes.");
			}

		}
	}

	@Override
	public void close() {
		Deserializer.super.close();
	}
}
