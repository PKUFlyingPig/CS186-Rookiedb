package edu.berkeley.cs186.database.query.disk;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.EmptyBacktrackingIterator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.List;

/**
 * A run represents a section of space on disk that we can append records to or
 * read from. This is useful for external sorting to store records while we
 * aren't using them and free up memory. Automatically buffers reads and writes
 * to minimize I/Os incurred.
 */
public class Run implements Iterable<Record> {
    // The transaction this run will be used within
    private TransactionContext transaction;
    // Under the hood we'll be storing all the records in a temporary table
    private String tempTableName;
    private Schema schema;

    public Run(TransactionContext transaction, Schema schema) {
        this.transaction = transaction;
        this.schema = schema;
    }

    /**
     * Adds a record to this run.
     * @param record the record to add
     */
    public void add(Record record) {
        if (this.tempTableName == null) {
            this.tempTableName = transaction.createTempTable(schema);
        }
        this.transaction.addRecord(this.tempTableName, record);
    }

    /**
     * Adds a list of records to this run.
     * @param records the records to add
     */
    public void addAll(List<Record> records) {
        for (Record record: records) this.add(record);
    }

    /**
     * @return an iterator over the records in this run
     */
    public BacktrackingIterator<Record> iterator() {
        if (this.tempTableName == null) return new EmptyBacktrackingIterator<>();
        return this.transaction.getRecordIterator(this.tempTableName);
    }

    /**
     * @return the name of the table containing this run's records
     */
    public String getTableName() {
        return this.tempTableName;
    }
}
