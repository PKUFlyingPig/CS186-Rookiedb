package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

class TransactionTableEntry {
    // Transaction object for the transaction.
    Transaction transaction;
    // lastLSN of transaction, or 0 if no log entries for the transaction exist.
    long lastLSN = 0;
    // map of transaction's savepoints
    private Map<String, Long> savepoints = new HashMap<>();

    TransactionTableEntry(Transaction transaction) {
        this.transaction = transaction;
    }

    void addSavepoint(String name) {
        savepoints.put(name, lastLSN);
    }

    long getSavepoint(String name) {
        if (!savepoints.containsKey(name)) {
            throw new NoSuchElementException("transaction " + transaction.getTransNum() + " has no savepoint " +
                                             name);
        }
        return savepoints.get(name);
    }

    void deleteSavepoint(String name) {
        if (!savepoints.containsKey(name)) {
            throw new NoSuchElementException("transaction " + transaction.getTransNum() + " has no savepoint " +
                                             name);
        }
        savepoints.remove(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        TransactionTableEntry that = (TransactionTableEntry) o;
        return lastLSN == that.lastLSN &&
               Objects.equals(transaction, that.transaction) &&
               Objects.equals(savepoints, that.savepoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction, lastLSN, savepoints);
    }

    @Override
    public String toString() {
        return "TransactionTableEntry{" +
               "transaction=" + transaction +
               ", lastLSN=" + lastLSN +
               '}';
    }
}
