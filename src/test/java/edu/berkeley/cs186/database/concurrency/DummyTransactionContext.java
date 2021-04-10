package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A dummy transaction class that only supports checking/setting active/blocked
 * status. Used for testing locking code without requiring an instance
 * of the database.
 */
public class DummyTransactionContext extends TransactionContext {
    private long tNum;
    private LoggingLockManager lockManager;

    public DummyTransactionContext(LoggingLockManager lockManager, long tNum) {
        this.lockManager = lockManager;
        this.tNum = tNum;
    }

    @Override
    public long getTransNum() {
        return this.tNum;
    }

    @Override
    public String createTempTable(Schema schema) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void deleteAllTempTables() {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void setAliasMap(Map<String, String> aliasMap) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void clearAliasMap() {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public boolean indexExists(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Iterator<Record> sortedScan(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Iterator<Record> sortedScanFrom(String tableName, String columnName,
                                           DataBox startValue) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Iterator<Record> lookupKey(String tableName, String columnName,
                                      DataBox key) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public boolean contains(String tableName, String columnName, DataBox key) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public RecordId addRecord(String tableName, Record record) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getWorkMemSize() {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public RecordId deleteRecord(String tableName, RecordId rid)  {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Record getRecord(String tableName, RecordId rid) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public BacktrackingIterator<Record> getRecordIterator(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public RecordId updateRecord(String tableName, RecordId rid, Record record)  {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void updateRecordWhere(String tableName, String targetColumnName,
                                  UnaryOperator<DataBox> targetValue,
                                  String predColumnName, PredicateOperator predOperator, DataBox predValue) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void updateRecordWhere(String tableName, String targetColumnName, Function<Record, DataBox> expr, Function<Record, DataBox> cond) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void deleteRecordWhere(String tableName, String predColumnName,
                                  PredicateOperator predOperator, DataBox predValue) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void deleteRecordWhere(String tableName, Function<Record, DataBox> cond) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public TableStats getStats(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getNumDataPages(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getTreeOrder(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getTreeHeight(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Schema getSchema(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Schema getFullyQualifiedSchema(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Table getTable(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void updateIndexMetadata(BPlusTreeMetadata metadata) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void block() {
        lockManager.emit("block " + tNum);
        super.block();
    }

    @Override
    public void unblock() {
        lockManager.emit("unblock " + tNum);
        super.unblock();
        Thread.yield();
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "Dummy Transaction #" + tNum;
    }
}

