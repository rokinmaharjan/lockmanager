package com.baylor.lockmanager.transaction;

import java.util.List;

public class TransactionService {

	public static Transaction getTransactionById(String id, List<Transaction> transactions) {
		for (Transaction transaction : transactions) {
			if (id.equals(transaction.getId())) {
				return transaction;
			}
		}

		return null;
	}
}
