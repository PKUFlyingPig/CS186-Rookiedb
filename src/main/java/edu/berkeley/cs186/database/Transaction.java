package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * The public-facing interface of a transaction.
 */
public abstract class Transaction implements AutoCloseable {
    // Status //////////////////////////////////////////////////////////////////
    public enum Status {
        RUNNING,
        COMMITTING,
        ABORTING,
        COMPLETE,
        RECOVERY_ABORTING; // "ABORTING" state for txns during restart recovery

        private static Status[] values = Status.values();

        public int getValue() {
            return ordinal();
        }

        public static Status fromInt(int x) {
            if (x < 0 || x >= values.length) {
                String err = String.format("Unknown TypeId ordinal %d.", x);
                throw new IllegalArgumentException(err);
            }
            return values[x];
        }
    }

    private Status status = Status.RUNNING;

    /**
     * Executes a statement (e.g. SELECT, UPDATE, INSERT, etc...)
     */
    public abstract Optional<QueryPlan> execute(String statement);

    /**
     * @return transaction number
     */
    public abstract long getTransNum();

    /**
     * @return current status of transaction
     */
    public final Status getStatus() {
        return status;
    }

    /**
     * Sets status of transaction. Should not be used directly by
     * users of the transaction (this should be called by the recovery
     * manager).
     * @param status new status of transaction
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Commits a transaction. Equivalent to
     *      COMMIT
     *
     * This is the default way a transaction ends.
     */
    public final void commit() {
        if (status != Status.RUNNING) {
            throw new IllegalStateException("transaction not in running state, cannot commit");
        }
        startCommit();
    }

    /**
     * Rolls back a transaction. Equivalent to
     *      ROLLBACK
     *
     * Project 5 (Recovery) must be fully implemented.
     */
    public final void rollback() {
        if (status != Status.RUNNING) {
            throw new IllegalStateException("transaction not in running state, cannot rollback");
        }
        startRollback();
    }

    /**
     * Cleanup transaction (when transaction ends). Does not
     * need to be called directly, as commit/rollback should
     * call cleanup themselves. Does not do anything on successive calls
     * when called multiple times.
     */
    public abstract void cleanup();

    /**
     * Implements close() as commit() when abort/commit not called - so that we can write:
     *
     * try (Transaction t = ...) {
     * ...
     * }
     *
     * and have the transaction commit.
     */
    @Override
    public final void close() {
        if (status == Status.RUNNING) {
            commit();
        }
    }

    // DDL /////////////////////////////////////////////////////////////////////

    /**
     * Creates a table. Equivalent to
     *      CREATE TABLE tableName (...s)
     *
     * Indices must be created afterwards with createIndex.
     *
     * @param s schema of new table
     * @param tableName name of new table
     */
    public abstract void createTable(Schema s, String tableName);

    /**
     * Drops a table. Equivalent to
     *      DROP TABLE tableName
     *
     * @param tableName name of table to drop
     */
    public abstract void dropTable(String tableName);

    /**
     * Drops all normal tables.
     */
    public abstract void dropAllTables();

    /**
     * Creates an index. Equivalent to
     *      CREATE INDEX tableName_columnName ON tableName (columnName)
     * in postgres.
     *
     * The only index supported is a B+ tree. Indices require Project 2 (B+ trees) to
     * be fully implemented. Bulk loading requires Project 3 Part 1 (Joins/Sorting) to be
     * fully implemented as well.
     *
     * @param tableName name of table to create index for
     * @param columnName name of column to create index on
     * @param bulkLoad whether to bulk load data
     */
    public abstract void createIndex(String tableName, String columnName, boolean bulkLoad);

    /**
     * Drops an index. Equivalent to
     *      DROP INDEX tableName_columnName
     * in postgres.
     *
     * @param tableName name of table to drop index from
     * @param columnName name of column to drop index from
     */
    public abstract void dropIndex(String tableName, String columnName);

    // DML /////////////////////////////////////////////////////////////////////

    /**
     * Returns a QueryPlan selecting from tableName. Equivalent to
     *      SELECT * FROM tableName
     * and used for all SELECT queries.
     * @param tableName name of table to select from
     * @return new query plan
     */
    public abstract QueryPlan query(String tableName);

    /**
     * Returns a QueryPlan selecting from tableName. Equivalent to
     *      SELECT * FROM tableName AS alias
     * and used for all SELECT queries.
     * @param tableName name of table to select from
     * @param alias alias of tableName
     * @return new query plan
     */
    public abstract QueryPlan query(String tableName, String alias);

    /**
     * Inserts a row into a table. Equivalent to
     *      INSERT INTO tableName VALUES(...values)`
     *
     * @param tableName name of table to insert into
     * @param values the values to be inserted. Can either be a sequence of
     *               types supported by the database or a
     */
    public void insert(String tableName, Object... values) {
        insert(tableName, new Record(values));
    }

    /**
     * Inserts a row into a table. Equivalent to
     *      INSERT INTO tableName VALUES(...values)
     * Using the values in `record`
     *
     * @param tableName name of table to insert into
     * @param record a record containing the values to be inserted
     */
    public abstract void insert(String tableName, Record record);

    /**
     * Updates rows in a table. Equivalent to
     *      UPDATE tableName SET targetColumnName = targetValue(targetColumnName)
     *
     * @param tableName name of table to update
     * @param targetColumnName column to update
     * @param targetValue function mapping old values to new values
     */
    public abstract void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue);

    /**
     * Updates rows in a table. Equivalent to
     *      UPDATE tableName SET targetColumnName = targetValue(targetColumnName)
     *       WHERE predColumnName predOperator predValue
     *
     * @param tableName name of table to update
     * @param targetColumnName column to update
     * @param targetValue function mapping old values to new values
     * @param predColumnName column used in WHERE predicate
     * @param predOperator operator used in WHERE predicate
     * @param predValue value used in WHERE predicate
     */
    public abstract void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue,
                                String predColumnName, PredicateOperator predOperator, DataBox predValue);

    /**
     * @param tableName name of table to update
     * @param targetColumnName column to update
     * @param expr expression that evaluates to the new updated values
     */
    public void update(String tableName, String targetColumnName, Function<Record, DataBox> expr) {
        update(tableName, targetColumnName, expr, (r) -> new BoolDataBox(true));
    };

    /**
     * @param tableName name of table to update
     * @param targetColumnName column to update
     * @param expr expression that evaluates to the new updated values
     * @param cond expression that is evaluated to determine if a given record
     *             should be updated.
     */
    public abstract void update(String tableName, String targetColumnName, Function<Record, DataBox> expr, Function<Record, DataBox> cond);

    /**
     * Deletes rows from a table. Equivalent to
     *      DELETE FROM tableNAME WHERE predColumnName predOperator predValue
     *
     * @param tableName name of table to delete from
     * @param predColumnName column used in WHERE predicate
     * @param predOperator operator used in WHERE predicate
     * @param predValue value used in WHERE predicate
     */
    public abstract void delete(String tableName, String predColumnName, PredicateOperator predOperator,
                DataBox predValue);

    /**
     * @param tableName name of table to delete from
     * @param cond expression that is evaluated to determine if a given record
     *             should be deleted based on its values
     */
    public abstract void delete(String tableName, Function<Record, DataBox> cond);

    // Savepoints //////////////////////////////////////////////////////////////

    /**
     * Creates a savepoint. A transaction may roll back to a savepoint it created
     * at any point before committing/aborting. Equivalent to
     *      SAVEPOINT savepointName
     *
     * Savepoints require Project 5 (recovery) to be fully implemented.
     *
     * @param savepointName name of savepoint
     */
    public abstract void savepoint(String savepointName);

    /**
     * Rolls back all changes made by the transaction since the given savepoint.
     * Equivalent to
     *      ROLLBACK TO savepointName
     *
     * Savepoints require Project 5 (recovery) to be fully implemented.
     *
     * @param savepointName name of savepoint
     */
    public abstract void rollbackToSavepoint(String savepointName);

    /**
     * Deletes a savepoint. Equivalent to
     *      RELEASE SAVEPOINT
     *
     * Savepoints require Project 5 (recovery) to be fully implemented.
     *
     * @param savepointName name of savepoint
     */
    public abstract void releaseSavepoint(String savepointName);

    // Schema //////////////////////////////////////////////////////////////////

    /**
     * @param tableName name of table to get schema of
     * @return schema of table
     */
    public abstract Schema getSchema(String tableName);

    // Internal ////////////////////////////////////////////////////////////////

    /**
     * @return transaction context for this transaction
     */
    public abstract TransactionContext getTransactionContext();

    /**
     * Called when commit() is called. Any exception thrown in this method will cause
     * the transaction to abort.
     */
    protected abstract void startCommit();

    /**
     * Called when rollback() is called. No exception should be thrown, and any exception
     * thrown will be interpreted the same as if the method had returned normally.
     */
    protected abstract void startRollback();
}
