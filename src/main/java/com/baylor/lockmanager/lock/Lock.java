package com.baylor.lockmanager.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.baylor.lockmanager.enums.LockType;

import lombok.Data;

@Data
public class Lock {
	private int dataId;
	// List<Map<transactionId, LockType>>
	private List<Map<String, LockType>> lockList = new ArrayList<Map<String,LockType>>();

}
