package com.shyam.kafka.customserializeranddeserializer;

import java.util.Date;

public class Supplier {

	private int supplierId;
	private String supplierName;
	private Date supplierStartDate;

	public Supplier() {
	}

	public Supplier(int id, String name, Date supplierDate) {
		this.supplierId = id;
		this.supplierName = name;
		this.supplierStartDate = supplierDate;
	}

	public int getSupplierId() {
		return supplierId;
	}

	public String getSupplierName() {
		return supplierName;
	}

	public Date getSupplierStartDate() {
		return supplierStartDate;
	}
}
