package com.baylor.lockmanager.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baylor.lockmanager.deadlock.WaitForGraph;
import com.baylor.lockmanager.enums.LockType;
import com.baylor.lockmanager.transaction.Transaction;

import lombok.Data;

public class Lock {
	private int dataId;

	// List<Map<transactionId, LockType>>
	private List<Map<String, LockType>> lockList = new ArrayList<Map<String, LockType>>();

	private Set<Transaction> owners = new HashSet<Transaction>();

	private final WaitForGraph waitForGraph;

	private Map<String, Integer> writeCount = new HashMap<String, Integer>();

	public Lock(WaitForGraph waitForGraph) {
		this.waitForGraph = waitForGraph;
	}

	public int getDataId() {
		return dataId;
	}

	public void setDataId(int dataId) {
		this.dataId = dataId;
	}

	public List<Map<String, LockType>> getLockList() {
		return lockList;
	}

	public void setLockList(List<Map<String, LockType>> lockList) {
		this.lockList = lockList;
	}

	public Set<Transaction> getOwners() {
		return owners;
	}

	public void setOwners(Set<Transaction> owners) {
		this.owners = owners;
	}

	public Map<String, Integer> getWriteCount() {
		return writeCount;
	}

	public void setWriteCount(Map<String, Integer> writeCount) {
		this.writeCount = writeCount;
	}

	public WaitForGraph getWaitForGraph() {
		return waitForGraph;
	}

	@Override
	public String toString() {
		return "Lock [dataId=" + dataId + ", lockList=" + lockList + ", owners=" + owners + ", waitForGraph="
				+ waitForGraph + ", writeCount=" + writeCount + "]";
	}

}
