package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Special operator that takes in a source, limit, and offset. The source's
 * input is advanced by the offset, after which up to records up to the
 * limit are yielded.
 */
public class LimitOperator extends QueryOperator {
    private int limit;
    private int offset;

    public LimitOperator(QueryOperator source, int limit, int offset) {
        super(OperatorType.LIMIT, source);
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    protected Schema computeSchema() {
        return source.outputSchema;
    }

    @Override
    public Iterator<Record> iterator() {
        return new LimitIterator(this.limit, this.offset);
    }

    @Override
    public String str() {
        return "Limit (cost=" + this.estimateIOCost() + ")";
    }

    @Override
    public TableStats estimateStats() {
        return this.source.estimateStats();
    }

    @Override
    public int estimateIOCost() {
        return 0;
    }

    @Override
    public List<String> sortedBy() { return getSource().sortedBy(); }

    private class LimitIterator implements Iterator<Record> {
        int limit;
        int offset;
        Iterator<Record> recordIterator;

        public LimitIterator(int limit, int offset) {
            this.limit = limit;
            this.offset = offset;
            this.recordIterator = source.iterator();
            while (this.offset > 0 && this.recordIterator.hasNext()) {
                this.offset--;
                this.recordIterator.next();
            }
        }

        @Override
        public boolean hasNext() {
            return this.limit != 0 && this.recordIterator.hasNext();
        }

        @Override
        public Record next() {
            if (!hasNext()) throw new NoSuchElementException();
            this.limit--;
            return this.recordIterator.next();
        }
    }

}
