package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;

/**
 * A MIN aggregate keeps track of the smallest value it has seen and return that
 * value as a result. Works for all data types and always returns the same data
 * type as the column being aggregated.
 */
public class MinAggregateFunction extends AggregateFunction {
    private Type colType;
    private DataBox min;

    public MinAggregateFunction(Type colType) {
        this.colType = colType;
    }

    @Override
    public void update(DataBox d) {
        if (min == null || d.compareTo(min) < 0) min = d;
    }

    @Override
    public DataBox getResult() {
        return min;
    }

    @Override
    public Type getResultType() {
        return this.colType;
    }

    @Override
    public void reset() {
        this.min = null;
    }

}
