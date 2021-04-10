package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SelectOperator extends QueryOperator {
    private int columnIndex;
    private String columnName;
    private PredicateOperator operator;
    private DataBox value;

    /**
     * Creates a new SelectOperator that pulls from source and only returns tuples for which the
     * predicate is satisfied.
     *
     * @param source the source of this operator
     * @param columnName the name of the column to evaluate the predicate on
     * @param operator the actual comparator
     * @param value the value to compare against
     */
    public SelectOperator(QueryOperator source,
                   String columnName,
                   PredicateOperator operator,
                   DataBox value) {
        super(OperatorType.SELECT, source);
        this.operator = operator;
        this.value = value;

        this.columnIndex = this.getSchema().findField(columnName);
        this.columnName = this.getSchema().getFieldName(columnIndex);

        this.stats = this.estimateStats();
    }

    @Override
    public boolean isSelect() {
        return true;
    }

    @Override
    public Schema computeSchema() {
        return this.getSource().getSchema();
    }

    @Override
    public String str() {
        return String.format("Select %s%s%s (cost=%d)",
                this.columnName, this.operator.toSymbol(), this.value, this.estimateIOCost());
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    @Override
    public TableStats estimateStats() {
        TableStats stats = this.getSource().estimateStats();
        return stats.copyWithPredicate(this.columnIndex,
                                       this.operator,
                                       this.value);
    }

    @Override
    public int estimateIOCost() {
        return this.getSource().estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() { return new SelectIterator(); }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class SelectIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private Record nextRecord;

        private SelectIterator() {
            this.sourceIterator = SelectOperator.this.getSource().iterator();
            this.nextRecord = null;
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            while (this.sourceIterator.hasNext()) {
                Record r = this.sourceIterator.next();
                switch (SelectOperator.this.operator) {
                case EQUALS:
                    if (r.getValue(SelectOperator.this.columnIndex).equals(value)) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case NOT_EQUALS:
                    if (!r.getValue(SelectOperator.this.columnIndex).equals(value)) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case LESS_THAN:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) < 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case LESS_THAN_EQUALS:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) < 0) {
                        this.nextRecord = r;
                        return true;
                    } else if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) == 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case GREATER_THAN:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) > 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case GREATER_THAN_EQUALS:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) > 0) {
                        this.nextRecord = r;
                        return true;
                    } else if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) == 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                default:
                    break;
                }
            }
            return false;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
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

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
