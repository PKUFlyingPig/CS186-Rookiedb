package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Internal transaction-specific methods, used for implementing parts of the database.
 *
 * The transaction context for the transaction currently running on the current thread
 * can be fetched via TransactionContext::getTransaction; it is only set during the middle
 * of a Transaction call.
 *
 * This transaction context implementation assumes that exactly one transaction runs
 * on a thread at a time, and that, aside from the unblock() method, no methods
 * of the transaction are called from a different thread than the thread that the
 * transaction is associated with. This implementation blocks the thread when
 * block() is called.
 */
public abstract class TransactionContext implements AutoCloseable {
    static Map<Long, List<TransactionContext>> threadTransactions = new ConcurrentHashMap<>();
    private boolean blocked = false;
    private boolean startBlock = false;
    private final ReentrantLock transactionLock = new ReentrantLock();
    private final Condition unblocked = transactionLock.newCondition();

    /**
     * Fetches the current transaction running on this thread.
     * @return transaction actively running on this thread or null if none
     */
    public static TransactionContext getTransaction() {
        long threadId = Thread.currentThread().getId();
        List<TransactionContext> transactions = threadTransactions.get(threadId);
        if (transactions != null && transactions.size() > 0) {
            return transactions.get(transactions.size() - 1);
        }
        return null;
    }

    /**
     * Sets the current transaction running on this thread.
     * @param transaction transaction currently running
     */
    public static void setTransaction(TransactionContext transaction) {
        long threadId = Thread.currentThread().getId();
        threadTransactions.putIfAbsent(threadId, new ArrayList<>());
        threadTransactions.get(threadId).add(transaction);
    }

    /**
     * Unsets the current transaction running on this thread.
     */
    public static void unsetTransaction() {
        long threadId = Thread.currentThread().getId();
        List<TransactionContext> transactions = threadTransactions.get(threadId);
        if (transactions == null || transactions.size() == 0) {
            throw new IllegalStateException("no transaction to unset");
        }
        transactions.remove(transactions.size() - 1);
    }

    // Status //////////////////////////////////////////////////////////////////

    /**
     * @return transaction number
     */
    public abstract long getTransNum();

    /**
     * @return the amount of memory (in pages) allocated for this transaction
     */
    public abstract int getWorkMemSize();

    @Override
    public abstract void close();

    // Temp Tables and Aliasing ////////////////////////////////////////////////
    /**
     * Create a temporary table within this transaction.
     *
     * @param schema the table schema
     * @return name of the tempTable
     */
    public abstract String createTempTable(Schema schema);

    /**
     * Deletes all temporary tables within this transaction.
     */
    public abstract void deleteAllTempTables();

    /**
     * Specify an alias mapping for this transaction. Recursive aliasing is
     * not allowed.
     * @param aliasMap mapping of alias names to original table names
     */
    public abstract void setAliasMap(Map<String, String> aliasMap);

    /**
     * Clears any aliases set.
     */
    public abstract void clearAliasMap();

    // Indices /////////////////////////////////////////////////////////////////

    /**
     * Perform a check to see if the database has an index on this (table,column).
     *
     * @param tableName  the name of the table
     * @param columnName the name of the column
     * @return boolean if the index exists
     */
    public abstract boolean indexExists(String tableName, String columnName);

    public abstract void updateIndexMetadata(BPlusTreeMetadata metadata);

    // Scans ///////////////////////////////////////////////////////////////////

    /**
     * Returns an iterator of records in `tableName` sorted in ascending of the
     * values in `columnName`.
     */
    public abstract Iterator<Record> sortedScan(String tableName, String columnName);

    /**
     * Returns an iterator of records in `tableName` sorted in ascending of the
     * values in `columnName`, including only records whose value in that column
     * is greater than or equal to `startValue`.
     */
    public abstract Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue);

    /**
     * Returns an iterator over the records in `tableName` where the value in
     * `columnName` are equal to `key`.
     */
    public abstract Iterator<Record> lookupKey(String tableName, String columnName, DataBox key);

    /**
     * Returns a backtracking iterator over all of the records in `tableName`.
     */
    public abstract BacktrackingIterator<Record> getRecordIterator(String tableName);

    public abstract boolean contains(String tableName, String columnName, DataBox key);

    // Record Operations ///////////////////////////////////////////////////////
    public abstract RecordId addRecord(String tableName, Record record);

    public abstract RecordId deleteRecord(String tableName, RecordId rid);

    public abstract void deleteRecordWhere(String tableName, String predColumnName, PredicateOperator predOperator,
                                           DataBox predValue);

    public abstract void deleteRecordWhere(String tableName, Function<Record, DataBox> cond);

    public abstract Record getRecord(String tableName, RecordId rid);

    public abstract RecordId updateRecord(String tableName, RecordId rid, Record updated);

    public abstract void updateRecordWhere(String tableName, String targetColumnName,
                                           UnaryOperator<DataBox> targetValue,
                                           String predColumnName, PredicateOperator predOperator, DataBox predValue);

    public abstract void updateRecordWhere(String tableName, String targetColumnName, Function<Record, DataBox> expr, Function<Record, DataBox> cond);

    // Table/Schema ////////////////////////////////////////////////////////////

    /**
     * @param tableName name of table to get schema of
     * @return schema of table
     */
    public abstract Schema getSchema(String tableName);

    /**
     * Same as getSchema, except all column names are fully qualified (tableName.colName).
     *
     * @param tableName name of table to get schema of
     * @return schema of table
     */
    public abstract Schema getFullyQualifiedSchema(String tableName);

    public abstract Table getTable(String tableName);

    // Statistics //////////////////////////////////////////////////////////////

    /**
     * @param tableName name of table to get stats of
     * @return TableStats object of the table
     */
    public abstract TableStats getStats(String tableName);

    /**
     * @param tableName name of table
     * @return number of data pages used by the table
     */
    public abstract int getNumDataPages(String tableName);

    /**
     * @param tableName name of table
     * @param columnName name of column
     * @return order of B+ tree index on tableName.columnName
     */
    public abstract int getTreeOrder(String tableName, String columnName);

    /**
     * @param tableName name of table
     * @param columnName name of column
     * @return height of B+ tree index on tableName.columnName
     */
    public abstract int getTreeHeight(String tableName, String columnName);

    // Synchronization /////////////////////////////////////////////////////////

    /**
     * prepareBlock acquires the lock backing the condition variable that the transaction
     * waits on. Must be called before block(), and is used to ensure that the unblock() call
     * corresponding to the following block() call cannot be run before the transaction blocks.
     */
    public void prepareBlock() {
        if (this.startBlock) {
            throw new IllegalStateException("already preparing to block");
        }
        this.transactionLock.lock();
        this.startBlock = true;
    }

    /**
     * Blocks the transaction (and thread). prepareBlock() must be called first.
     */
    public void block() {
        if (!this.startBlock) {
            throw new IllegalStateException("prepareBlock() must be called before block()");
        }
        try {
            this.blocked = true;
            while (this.blocked) {
                this.unblocked.awaitUninterruptibly();
            }
        } finally {
            this.startBlock = false;
            this.transactionLock.unlock();
        }
    }

    /**
     * Unblocks the transaction (and thread running the transaction).
     */
    public void unblock() {
        this.transactionLock.lock();
        try {
            this.blocked = false;
            this.unblocked.signal();
        } finally {
            this.transactionLock.unlock();
        }
    }

    /**
     * @return if the transaction is blocked
     */
    public boolean getBlocked() {
        return this.blocked;
    }
}
