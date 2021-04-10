package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class IndexScanOperator extends QueryOperator {
    private TransactionContext transaction;
    private String tableName;
    private String columnName;
    private PredicateOperator predicate;
    private DataBox value;

    private int columnIndex;

    /**
     * An index scan operator.
     *
     * @param transaction the transaction containing this operator
     * @param tableName the table to iterate over
     * @param columnName the name of the column the index is on
     */
    IndexScanOperator(TransactionContext transaction,
                      String tableName,
                      String columnName,
                      PredicateOperator predicate,
                      DataBox value) {
        super(OperatorType.INDEX_SCAN);
        this.tableName = tableName;
        this.transaction = transaction;
        this.columnName = columnName;
        this.predicate = predicate;
        this.value = value;
        this.setOutputSchema(this.computeSchema());
        this.columnIndex = this.getSchema().findField(columnName);
        this.stats = this.estimateStats();
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public String str() {
        return String.format("Index Scan for %s%s%s on %s (cost=%d)",
            this.columnName, this.predicate.toSymbol(), this.value, this.tableName,
            this.estimateIOCost());
    }

    /**
     * Returns the column name that the index scan is on
     *
     * @return columnName
     */
    public String getColumnName() {
        return this.columnName;
    }

    @Override
    public TableStats estimateStats() {
        TableStats stats = this.transaction.getStats(this.tableName);
        return stats.copyWithPredicate(this.columnIndex,
                                       this.predicate,
                                       this.value);
    }

    @Override
    public int estimateIOCost() {
        int height = transaction.getTreeHeight(tableName, columnName);
        int order = transaction.getTreeOrder(tableName, columnName);
        TableStats tableStats = transaction.getStats(tableName);

        int count = tableStats.getHistograms().get(columnIndex).copyWithPredicate(predicate,
                    value).getCount();
        // 2 * order entries/leaf node, but leaf nodes are 50-100% full; we use a fill factor of
        // 75% as a rough estimate
        return (int) (height + Math.ceil(count / (1.5 * order)) + count);
    }

    @Override
    public Iterator<Record> iterator() {
        return new IndexScanIterator();
    }

    @Override
    public Schema computeSchema() {
        return this.transaction.getFullyQualifiedSchema(this.tableName);
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(this.columnName);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class IndexScanIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private Record nextRecord;

        private IndexScanIterator() {
            this.nextRecord = null;
            if (IndexScanOperator.this.predicate == PredicateOperator.EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.lookupKey(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
            } else if (IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN ||
                       IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN_EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScan(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName);
            } else if (IndexScanOperator.this.predicate == PredicateOperator.GREATER_THAN) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
                while (this.sourceIterator.hasNext()) {
                    Record r = this.sourceIterator.next();

                    if (r.getValue(IndexScanOperator.this.columnIndex)
                            .compareTo(IndexScanOperator.this.value) > 0) {
                        this.nextRecord = r;
                        break;
                    }
                }
            } else if (IndexScanOperator.this.predicate == PredicateOperator.GREATER_THAN_EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord != null) return true;
            if (!this.sourceIterator.hasNext()) return false;
            Record r = this.sourceIterator.next();
            if (predicate == PredicateOperator.LESS_THAN) {
                if (r.getValue(columnIndex).compareTo(value) < 0) {
                    this.nextRecord = r;
                }
            } else if (predicate == PredicateOperator.LESS_THAN_EQUALS) {
                if (r.getValue(columnIndex).compareTo(value) <= 0) {
                    this.nextRecord = r;
                }
            } else  {
                this.nextRecord = r;
            }
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }
    }
}
