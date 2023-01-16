package com.baylor.lockmanager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.map.HashedMap;

import com.baylor.lockmanager.deadlock.WaitForGraph;
import com.baylor.lockmanager.enums.LockType;
import com.baylor.lockmanager.enums.OperationType;
import com.baylor.lockmanager.lock.Lock;
import com.baylor.lockmanager.lock.LockService;
import com.baylor.lockmanager.transaction.Transaction;
import com.baylor.lockmanager.transaction.TransactionService;
import com.baylor.lockmanager.utils.StringUtils;

public class Main {

	private static List<Transaction> activeTransactions = new ArrayList<Transaction>();
	private static List<Lock> locks = new ArrayList<Lock>();
	private static String[] dbValue = new String[32];
	private static WaitForGraph waitForGraph = new WaitForGraph();

	private static void createLocksForAllDataId() {
		for (int i = 0; i < 32; i++) {
			Lock lock = new Lock(waitForGraph);
			lock.setDataId(i);

			locks.add(lock);
		}
	}

	private static void handleStart(String transactionId) {
		Transaction transaction = TransactionService.getTransactionById(transactionId, activeTransactions);

		if (transaction == null) {
			transaction = new Transaction(transactionId);

			activeTransactions.add(transaction);
		}
	}

	@SuppressWarnings({ "unchecked", "serial" })
	private static void handleRead(String dataIdString, String transactionId) {
		Transaction transaction = TransactionService.getTransactionById(transactionId, activeTransactions);

		int dataId = Integer.valueOf(dataIdString);

		Lock lock = LockService.getLockByDataId(dataId, locks);

		if (lock.getLockList().size() == 0) {
			System.out.println(String.format("Output of Read %s %s: %s", dataIdString, transactionId, dbValue[dataId]));
			lock.getOwners().add(transaction);

			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.READ);
				}
			});

			return;
		}

		boolean writeLockPresent = LockService.checkIfWriteLockPresent(lock);
		if (!writeLockPresent) {
			System.out.println(String.format("Output of Read %s %s: %s", dataIdString, transactionId, dbValue[dataId]));
			lock.getOwners().add(transaction);

			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.READ);
				}
			});

			return;
		} else {

			WaitForGraph wfg = lock.getWaitForGraph();
			wfg.add(transaction, lock.getOwners());
			boolean deadlockDetected = wfg.detectDeadlock(transaction);

			if (deadlockDetected) {
				System.out.println(String.format("Deadlock detected!! Initiating rollback"));
				// ROLLBACK HERE
				handleRollback(transactionId);

			} else {
				lock.getLockList().add(new HashedMap() {
					{
						put(transactionId, LockType.READ);
					}
				});
			}

			return;
		}
	}

	@SuppressWarnings({ "unchecked", "serial" })
	private static void handleWrite(String dataIdString, String transactionId) {
		Transaction transaction = TransactionService.getTransactionById(transactionId, activeTransactions);

		int dataId = Integer.valueOf(dataIdString);

		Lock lock = LockService.getLockByDataId(dataId, locks);

		if (lock.getLockList().size() == 0) {
			dbValue[dataId] = StringUtils.flipBit(dbValue[dataId]);

			Map<String, Integer> writeCount = lock.getWriteCount();

			int writeCountInt = 0;
			if (!writeCount.isEmpty() && !(writeCount.get(transactionId) == null)) {
				writeCountInt = writeCount.get(transactionId);
			}

			writeCount.put(transactionId, writeCountInt + 1);

			System.out.println(
					String.format("Output of Write %s %s: %s", dataIdString, transactionId, Arrays.toString(dbValue)));
			lock.getOwners().add(transaction);

			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.WRITE);
				}
			});

			return;
		} else {
			WaitForGraph wfg = lock.getWaitForGraph();
			wfg.add(transaction, lock.getOwners());
			boolean deadlockDetected = wfg.detectDeadlock(transaction);

			if (deadlockDetected) {
				System.out.println(String.format("Deadlock detected!! Initiaing rollback"));
				// ROLLBACK HERE
				handleRollback(transactionId);

			} else {
				lock.getLockList().add(new HashedMap() {
					{
						put(transactionId, LockType.WRITE);
					}
				});
			}

			return;
		}

	}

	@SuppressWarnings({ "unchecked", "serial" })
	private static void handleCommit(String transactionId) {
		List<Lock> locksContainingLockList = LockService.getLocksContainingLockList(locks);

		for (Lock lock : locksContainingLockList) {
			List<Map<String, LockType>> lockListClone = new ArrayList<Map<String, LockType>>(lock.getLockList());

			List<Map<String, LockType>> updatedLockList = new ArrayList<Map<String, LockType>>();

			for (Map<String, LockType> transactionLockMap : lockListClone) {
				Map.Entry<String, LockType> firstTransactionLockMap = transactionLockMap.entrySet().iterator().next();

				if (!firstTransactionLockMap.getKey().equals(transactionId)) {
					updatedLockList.add(new HashedMap() {
						{
							put(firstTransactionLockMap.getKey(), firstTransactionLockMap.getValue());
						}
					});

				}
			}

			lock.setLockList(updatedLockList);
		}

//		List<Map<String, LockType>> lockListClone = new ArrayList<Map<String, LockType>>(lock.getLockList());

		for (Lock lock : locksContainingLockList) {
			List<Map<String, LockType>> lockListClone = new ArrayList<Map<String, LockType>>(lock.getLockList());
			lock.setLockList(new ArrayList<Map<String, LockType>>());

			for (Map<String, LockType> transactionLockMap : lockListClone) {
				Map.Entry<String, LockType> firstTransactionLockMap = transactionLockMap.entrySet().iterator().next();

				if (firstTransactionLockMap.getValue() == LockType.READ) {
					handleRead(String.valueOf(lock.getDataId()), firstTransactionLockMap.getKey());
				} else {
					handleWrite(String.valueOf(lock.getDataId()), firstTransactionLockMap.getKey());
				}

//				if (!firstTransactionLockMap.getKey().equals(transactionId)) {
//					updatedLockList.add(new HashedMap() {
//						{
//							put(firstTransactionLockMap.getKey(), firstTransactionLockMap.getValue());
//						}
//					});
//
//				}
			}
		}

	}

	@SuppressWarnings({ "unchecked", "serial" })
	private static void handleRollback(String transactionId) {
		List<Lock> locksContainingLockList = LockService.getLocksContainingLockList(locks);

		for (Lock lock : locksContainingLockList) {
			List<Map<String, LockType>> lockListClone = new ArrayList<Map<String, LockType>>(lock.getLockList());

			List<Map<String, LockType>> updatedLockList = new ArrayList<Map<String, LockType>>();

			for (Map<String, LockType> transactionLockMap : lockListClone) {
				Map.Entry<String, LockType> firstTransactionLockMap = transactionLockMap.entrySet().iterator().next();

				if (!firstTransactionLockMap.getKey().equals(transactionId)) {
					updatedLockList.add(new HashedMap() {
						{
							put(firstTransactionLockMap.getKey(), firstTransactionLockMap.getValue());
						}
					});
				}
			}

			lock.setLockList(updatedLockList);
		}

		for (Lock lock : locksContainingLockList) {
			Map<String, Integer> m = lock.getWriteCount();
			int writeCount = 0;
			if (!m.isEmpty()) {
				writeCount = m.get(transactionId);
			}

			for (int i = 0; i < writeCount; i++) {
				dbValue[lock.getDataId()] = StringUtils.flipBit(dbValue[lock.getDataId()]);
			}
		}

		// Remove from active transactions
//		activeTransactions.remove(locksContainingLockList);
	}

	private static void handleEOF() {
		List<Lock> locksContainingLockList = LockService.getLocksContainingLockList(locks);

		System.out.println("===========================================================");
		System.out.println("Lock tables shown below: ");
		for (Lock lock : locksContainingLockList) {
			if (lock.getLockList().size() > 0) {
				System.out.println(
						String.format("Final lock table for dataId '%s':  %s", lock.getDataId(), lock.getLockList()));
			}
			System.out.println("-----------------------------------------------------------");
		}

		System.out.println("===========================================================");
		System.out.println("Transaction dependencies shown below: ");
		for (Map.Entry<Transaction, Set<Transaction>> entry : waitForGraph.adjacencyList.entrySet()) {
			Transaction key = entry.getKey();

			Set<Transaction> value = entry.getValue();

			System.out.println(String.format("Transaction Id: %s", key.getId()));
			for (Transaction transaction : value) {
				System.out.println(transaction.getId());
			}

			System.out.println("-----------------------------------------------------------");
		}

		System.out.println("===========================================================");
		System.out.println(String.format("Final value in db: %s", Arrays.toString(dbValue)));
	}

	private static void performOpeartions(List<String> operations) {
		for (String operation : operations) {
			String[] operationArray = operation.split(" ");
			OperationType operationType = OperationType.valueOf(operationArray[0]);

			switch (operationType) {
			case Start:
				handleStart(operationArray[1]);
				break;

			case Read:
				handleRead(operationArray[1], operationArray[2]);
				break;

			case Write:
				handleWrite(operationArray[1], operationArray[2]);
				break;

			case Commit:
				handleCommit(operationArray[1]);
				break;

			case Rollback:
				handleRollback(operationArray[1]);
				break;

			case EOF:
				handleEOF();
				break;

			default:
				break;
			}
		}
	}

	public static void main(String[] args) throws IOException {
		String inputFilePath = args[0];
		final List<String> input = Files.lines(Paths.get(inputFilePath)).collect(Collectors.toList());

		int initialValue = Integer.valueOf(input.get(0));
		String initialValueBinary = StringUtils.convertIntToBinary(initialValue);

		dbValue = initialValueBinary.split("");
		Collections.reverse(Arrays.asList(dbValue));

		System.out.println(String.format("Initial value in db: %s", Arrays.toString(dbValue)));
		System.out.println("===========================================================");

		input.remove(0);

		createLocksForAllDataId();

		performOpeartions(input);
	}
}
