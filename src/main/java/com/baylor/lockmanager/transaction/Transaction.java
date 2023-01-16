package com.baylor.lockmanager.transaction;

public class Transaction implements Comparable{

	private String id;

	public Transaction(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
