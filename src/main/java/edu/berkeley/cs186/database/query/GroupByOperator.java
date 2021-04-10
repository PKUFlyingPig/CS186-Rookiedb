package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

class GroupByOperator extends QueryOperator {
    static final Record MARKER = new Record();
    private List<Integer> groupByColumnIndices;
    private List<String> groupByColumns;
    private TransactionContext transaction;

    /**
     * Create a new GroupByOperator that pulls from source and groups by groupByColumn.
     *
     * @param source the source operator of this operator
     * @param transaction the transaction containing this operator
     * @param columns the columns to group on
     */
    GroupByOperator(QueryOperator source,
                    TransactionContext transaction,
                    List<String> columns) {
        super(OperatorType.GROUP_BY, source);
        Schema sourceSchema = this.getSource().getSchema();
        this.transaction = transaction;
        this.groupByColumns = new ArrayList<>();
        this.groupByColumnIndices = new ArrayList<>();
        for (String column: columns) {
            this.groupByColumns.add(sourceSchema.matchFieldName(column));
        }
        for (String groupByColumn: this.groupByColumns) {
            this.groupByColumnIndices.add(sourceSchema.getFieldNames().indexOf(groupByColumn));
        }

        this.stats = this.estimateStats();
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Iterator<Record> iterator() {
        return new GroupByIterator();
    }

    @Override
    protected Schema computeSchema() {
        return this.getSource().getSchema();
    }

    @Override
    public String str() {
        String columns;
        if (this.groupByColumns.size() == 1) columns = groupByColumns.get(0);
        else columns = "(" + String.join(", ", groupByColumns) + ")";
        return "Group By (cost=" + this.estimateIOCost() + ")" +
               "\n  columns: " + columns;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    @Override
    public TableStats estimateStats() {
        return this.getSource().estimateStats();
    }

    @Override
    public int estimateIOCost() {
        return this.getSource().estimateIOCost();
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * Returns a marker record between the records of different groups, e.g.
     * [group 1 record] [group 1 record] [marker record] [group 2 record] ...
     */
    private class GroupByIterator implements Iterator<Record> {
        private Map<Record, String> hashGroupTempTables;
        private int currCount;
        private Iterator<String> tableNames;
        private Iterator<Record> recordIterator;

        private GroupByIterator() {
            Iterator<Record> sourceIterator = GroupByOperator.this.getSource().iterator();
            this.hashGroupTempTables = new HashMap<>();
            this.currCount = 0;
            this.recordIterator = null;
            while (sourceIterator.hasNext()) {
                Record record = sourceIterator.next();
                List<DataBox> values = new ArrayList<>();
                for (int index: groupByColumnIndices) {
                    values.add(record.getValue(index));
                }
                Record key = new Record(values);
                String tableName;
                if (this.hashGroupTempTables.containsKey(key)) {
                    tableName = this.hashGroupTempTables.get(key);;
                } else {
                    tableName = GroupByOperator.this.transaction.createTempTable(
                            GroupByOperator.this.getSource().getSchema());
                    this.hashGroupTempTables.put(key, tableName);
                }
                GroupByOperator.this.transaction.addRecord(tableName, record);
            }
            this.tableNames = hashGroupTempTables.values().iterator();
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            return this.tableNames.hasNext() || (this.recordIterator != null && this.recordIterator.hasNext());
        }

        /**
         * @return the next record
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            while (this.hasNext()) {
                if (this.recordIterator != null && this.recordIterator.hasNext()) {
                    return this.recordIterator.next();
                } else if (this.tableNames.hasNext()) {
                    String tableName = this.tableNames.next();
                    Iterator<Record> prevIter = this.recordIterator;
                    this.recordIterator = GroupByOperator.this.transaction.getRecordIterator(tableName);
                    if (prevIter != null && ++this.currCount < this.hashGroupTempTables.size()) {
                        return MARKER;
                    }
                }
            }
            throw new NoSuchElementException();
        }
    }
}
