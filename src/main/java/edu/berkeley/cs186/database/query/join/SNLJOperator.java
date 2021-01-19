package edu.berkeley.cs186.database.query.join;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Simple Nested Loop Join algorithm.
 */
public class SNLJOperator extends JoinOperator {
    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
              leftColumnName, rightColumnName, transaction, JoinType.SNLJ);
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        int numLeftRecords = getLeftSource().estimateStats().getNumRecords();

        int numRightPages = getRightSource().estimateStats().getNumPages();
        int numLeftPages = getLeftSource().estimateStats().getNumPages();

        return numLeftRecords * numRightPages + numLeftPages;
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Note that the left table is the "outer" loop and the right table is the
     * "inner" loop.
     */
    private class SNLJIterator implements Iterator<Record> {
        // Iterator over pages of the left relation
        private Iterator<Record> leftRecordIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Record> rightRecordIterator;
        // The current record on the left page
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        public SNLJIterator() {
            super();
            this.leftRecordIterator = getLeftSource().iterator();
            if (leftRecordIterator.hasNext()) leftRecord = leftRecordIterator.next();

            this.rightRecordIterator = getRightSource().backtrackingIterator();
            this.rightRecordIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            if (leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }
            while(true) {
                if (this.rightRecordIterator.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightRecordIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftRecordIterator.hasNext()){
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    this.leftRecord = leftRecordIterator.next();
                    this.rightRecordIterator.reset();
                } else {
                    // f you're here then there are no more records to fetch
                    return null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }

}

