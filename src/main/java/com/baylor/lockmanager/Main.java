package com.baylor.lockmanager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.map.HashedMap;

import com.baylor.lockmanager.enums.LockType;
import com.baylor.lockmanager.enums.OperationType;
import com.baylor.lockmanager.lock.Lock;
import com.baylor.lockmanager.lock.LockService;
import com.baylor.lockmanager.transaction.Transaction;
import com.baylor.lockmanager.transaction.TransactionService;
import com.baylor.lockmanager.utils.StringUtils;

public class Main {

	private static final String INPUT_FILE_NAME = "operations.txt";

	private static List<Transaction> activeTransactions = new ArrayList<Transaction>();
	private static List<Lock> locks = new ArrayList<Lock>();
	private static String[] dbValue = new String[32];

	private static void createLocksForAllDataId() {
		for (int i = 0; i < 32; i++) {
			Lock lock = new Lock();
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
		int dataId = Integer.valueOf(dataIdString);

		Lock lock = LockService.getLockByDataId(dataId, locks);

		if (lock.getLockList().size() == 0) {
			System.out.println(String.format("Output of Read %s %s: %s", dataIdString, transactionId, dbValue[dataId]));

			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.READ);
				}
			});

			System.out.println("DataId: " + dataIdString + " " + lock.getLockList());

			return;
		}

		boolean writeLockPresent = LockService.checkIfWriteLockPresent(lock);
		if (!writeLockPresent) {
			System.out.println(String.format("Output of Read %s %s: %s", dataIdString, transactionId, dbValue[dataId]));

			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.READ);
				}
			});

			System.out.println("DataId: " + dataIdString + " " + lock.getLockList());

			return;
		} else {
			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.READ);
				}
			});

			System.out.println("DataId: " + dataIdString + " " + lock.getLockList());

			return;
		}
	}

	@SuppressWarnings({ "unchecked", "serial" })
	private static void handleWrite(String dataIdString, String transactionId) {
		int dataId = Integer.valueOf(dataIdString);

		Lock lock = LockService.getLockByDataId(dataId, locks);

		if (lock.getLockList().size() == 0) {
			dbValue[dataId] = StringUtils.flipBit(dbValue[dataId]);

			System.out.println(
					String.format("Output of Write %s %s: %s", dataIdString, transactionId, Arrays.toString(dbValue)));

			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.WRITE);
				}
			});

			System.out.println("DataId: " + dataIdString + " " + lock.getLockList());

			return;
		} else {
			lock.getLockList().add(new HashedMap() {
				{
					put(transactionId, LockType.WRITE);
				}
			});

			System.out.println("DataId: " + dataIdString + " " + lock.getLockList());

			return;
		}
	}

	private static void executeReadAfterCommit(int dataId, String transactionId) {

	}

	@SuppressWarnings({ "unchecked", "serial" })
	private static void handleCommit(String transactionId) {
		List<Lock> locksContainingLockList = LockService.getLocksContainingLockList(locks);

		for (Lock lock : locksContainingLockList) {
			List<Map<String, LockType>> lockListClone = new ArrayList<Map<String, LockType>>(lock.getLockList());

			List<Map<String, LockType>> updatedLockList = new ArrayList<Map<String, LockType>>();

			boolean grantNext = false;
			for (Map<String, LockType> transactionLockMap : lockListClone) {
				Map.Entry<String, LockType> firstTransactionLockMap = transactionLockMap.entrySet().iterator().next();

				if (!firstTransactionLockMap.getKey().equals(transactionId)) {
					updatedLockList.add(new HashedMap() {
						{
							put(firstTransactionLockMap.getKey(), firstTransactionLockMap.getValue());
						}
					});

					grantNext = true;
				}

				if (!firstTransactionLockMap.getKey().equals(transactionId) && grantNext) {
					switch (firstTransactionLockMap.getValue()) {
					case READ:
						executeReadAfterCommit(lock.getDataId(), transactionId);

						grantNext = false;
						break;

//					case WRITE:
//						handleWrite(String.valueOf(lock.getDataId()), transactionId);
//						grantNext = false;
//						break;
					default:
						break;
					}
				}
			}

			lock.setLockList(updatedLockList);

		}

	}

	private static void handleRollback(String string) {
		// TODO Auto-generated method stub

	}

	private static void handleEOF() {
		List<Lock> locksContainingLockList = LockService.getLocksContainingLockList(locks);

		for (Lock lock : locksContainingLockList) {
			if (lock.getLockList().size() > 0) {
				System.out.println(
						String.format("Final lock table for dataId '%s':  %s", lock.getDataId(), lock.getLockList()));
			}
		}
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
		final List<String> input = Files.lines(Paths.get(INPUT_FILE_NAME)).collect(Collectors.toList());

		input.forEach(System.out::println);
		System.out.println("=====================");

		int initialValue = Integer.valueOf(input.get(0));
		String initialValueBinary = StringUtils.convertIntToBinary(initialValue);

		dbValue = initialValueBinary.split("");
		Collections.reverse(Arrays.asList(dbValue));

		input.remove(0);

		createLocksForAllDataId();

		performOpeartions(input);
		System.out.println(String.format("Final value in db: %s", Arrays.toString(dbValue)));

	}
}
