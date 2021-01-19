package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.*;

/**
 * A RANGE aggregate keeps track of the largest and smallest values it has seen,
 * and returns the difference as a result. Undefined for the STRING and BOOL
 * data types. Always returns the same data type as the column being
 * aggregated.
 */
public class RangeAggregateFunction extends AggregateFunction {
    MaxAggregateFunction maxAgg;
    MinAggregateFunction minAgg;

    public RangeAggregateFunction(Type colType) {
        if (colType.getTypeId() == TypeId.STRING || colType.getTypeId() == TypeId.BOOL) {
            throw new IllegalArgumentException("Invalid data type for RANGE aggregate: " + colType.getTypeId());
        }
        this.maxAgg = new MaxAggregateFunction(colType);
        this.minAgg = new MinAggregateFunction(colType);
    }

    @Override
    public void update(DataBox d) {
        this.maxAgg.update(d);
        this.minAgg.update(d);
    }

    @Override
    public Type getResultType() {
        return this.maxAgg.getResultType();
    }

    @Override
    public DataBox getResult() {
        DataBox max = maxAgg.getResult();
        DataBox min = minAgg.getResult();
        switch (max.getTypeId()) {
            case INT: return new IntDataBox(max.getInt() - min.getInt());
            case LONG: return new LongDataBox(max.getLong() - min.getLong());
            case FLOAT: return new FloatDataBox(max.getFloat() - min.getFloat());
        }
        throw new IllegalStateException("Unreachable code.");
    }

    @Override
    public void reset() {
        maxAgg.reset();
        minAgg.reset();
    }
}
