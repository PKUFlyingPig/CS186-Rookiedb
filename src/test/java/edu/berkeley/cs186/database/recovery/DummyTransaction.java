package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Dummy transaction that does nothing except maintain transaction
 * number and status.
 */
class DummyTransaction extends Transaction {
    private long transNum;
    boolean cleanedUp = false;

    private static Map<Long, DummyTransaction> transactions = new HashMap<>();

    private DummyTransaction(long transNum) {
        this.transNum = transNum;
    }

    static DummyTransaction create(long transNum) {
        if (!transactions.containsKey(transNum)) {
            transactions.put(transNum, new DummyTransaction(transNum));
        }
        return transactions.get(transNum);
    }

    static void cleanupTransactions() {
        for (DummyTransaction transaction : transactions.values()) {
            transaction.cleanedUp = false;
            transaction.setStatus(Status.RUNNING);
        }
    }

    @Override
    public String toString() {
        return "DummyTransaction #" + transNum;
    }

    @Override
    protected void startCommit() {}

    @Override
    protected void startRollback() {}

    @Override
    public void cleanup() {
        assert (!cleanedUp && getStatus() != Status.COMPLETE);
        cleanedUp = true;
    }

    @Override
    public Optional<QueryPlan> execute(String statement) {
        return Optional.empty();
    }

    @Override
    public long getTransNum() {
        return transNum;
    }

    @Override
    public void createTable(Schema s, String tableName) {}

    @Override
    public void dropTable(String tableName) {}

    @Override
    public void dropAllTables() {}

    @Override
    public void createIndex(String tableName, String columnName, boolean bulkLoad) {}

    @Override
    public void dropIndex(String tableName, String columnName) {}

    @Override
    public QueryPlan query(String tableName) {
        return null;
    }

    @Override
    public QueryPlan query(String tableName, String alias) {
        return null;
    }

    @Override
    public void insert(String tableName, Record values) {}

    @Override
    public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue) {}

    @Override
    public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue,
                       String predColumnName, PredicateOperator predOperator, DataBox predValue) {}

    @Override
    public void update(String tableName, String targetColumnName, Function<Record, DataBox> expr, Function<Record, DataBox> cond) { }

    @Override
    public void delete(String tableName, String predColumnName, PredicateOperator predOperator,
                       DataBox predValue) {}

    @Override
    public void delete(String tableName, Function<Record, DataBox> cond) { }

    @Override
    public void savepoint(String savepointName) {}

    @Override
    public void rollbackToSavepoint(String savepointName) {}

    @Override
    public void releaseSavepoint(String savepointName) {}

    @Override
    public Schema getSchema(String tableName) {
        return null;
    }

    @Override
    public TransactionContext getTransactionContext() {
        return new DummyTransactionContext();
    }

    private class DummyTransactionContext extends TransactionContext {
        @Override
        public long getTransNum() {
            return transNum;
        }

        @Override
        public int getWorkMemSize() {
            return 0;
        }

        @Override
        public void close() {}

        @Override
        public String createTempTable(Schema schema) {
            return null;
        }

        @Override
        public void deleteAllTempTables() {}

        @Override
        public void setAliasMap(Map<String, String> aliasMap) {}

        @Override
        public void clearAliasMap() {}

        @Override
        public boolean indexExists(String tableName, String columnName) {
            return false;
        }

        @Override
        public void updateIndexMetadata(BPlusTreeMetadata metadata) {}

        @Override
        public Iterator<Record> sortedScan(String tableName, String columnName) {
            return null;
        }

        @Override
        public Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue) {
            return null;
        }

        @Override
        public Iterator<Record> lookupKey(String tableName, String columnName, DataBox key) {
            return null;
        }

        @Override
        public BacktrackingIterator<Record> getRecordIterator(String tableName) {
            return null;
        }

        @Override
        public boolean contains(String tableName, String columnName, DataBox key) {
            return false;
        }

        @Override
        public RecordId addRecord(String tableName, Record record) {
            return null;
        }

        @Override
        public RecordId deleteRecord(String tableName, RecordId rid) {
            return null;
        }

        @Override
        public Record getRecord(String tableName, RecordId rid) {
            return null;
        }

        @Override
        public RecordId updateRecord(String tableName, RecordId rid, Record record) {
            return null;
        }

        @Override
        public void updateRecordWhere(String tableName, String targetColumnName,
                                      UnaryOperator<DataBox> targetValue, String predColumnName, PredicateOperator predOperator,
                                      DataBox predValue) {}

        @Override
        public void updateRecordWhere(String tableName, String targetColumnName, Function<Record, DataBox> expr, Function<Record, DataBox> cond) { }

        @Override
        public void deleteRecordWhere(String tableName, String predColumnName,
                                      PredicateOperator predOperator, DataBox predValue) {}

        @Override
        public void deleteRecordWhere(String tableName, Function<Record, DataBox> cond) { }

        @Override
        public Schema getSchema(String tableName) {
            return null;
        }

        @Override
        public Schema getFullyQualifiedSchema(String tableName) {
            return null;
        }

        @Override
        public Table getTable(String tableName) {
            return null;
        }

        @Override
        public TableStats getStats(String tableName) {
            return null;
        }

        @Override
        public int getNumDataPages(String tableName) {
            return 0;
        }

        @Override
        public int getTreeOrder(String tableName, String columnName) {
            return 0;
        }

        @Override
        public int getTreeHeight(String tableName, String columnName) {
            return 0;
        }
    }
}
