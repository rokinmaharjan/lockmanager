package com.baylor.lockmanager.lock;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.baylor.lockmanager.enums.LockType;

public class LockService {

	public static Lock getLockByDataId(int dataId, List<Lock> locks) {
		for (Lock lock : locks) {
			if (dataId == lock.getDataId()) {
				return lock;
			}
		}

		return null;
	}
	
	public static List<Lock> getLocksContainingLockList(List<Lock> locks) {
		return locks.stream().filter(l -> l.getLockList().size() > 0).collect(Collectors.toList());
	}

	public static boolean checkIfWriteLockPresent(Lock lock) {

		for (Map<String, LockType> transactionLock : lock.getLockList()) {

			Map.Entry<String, LockType> firstTransactionLock = transactionLock.entrySet().iterator().next();

			LockType lockType = firstTransactionLock.getValue();

			if (lockType == LockType.WRITE) {
				return true;
			}
		}

		return false;
	}
}
