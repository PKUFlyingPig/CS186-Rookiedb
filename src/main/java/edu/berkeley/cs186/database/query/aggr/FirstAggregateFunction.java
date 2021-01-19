package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;

/**
 * A FIRST aggregate keeps track of the first value it has seen and returns that
 * as a result. Works for all data types, and always returns the same data type
 * as the column being aggregated on.
 */
public class FirstAggregateFunction extends AggregateFunction {
    Type colType;
    DataBox first;

    public FirstAggregateFunction(Type columnType) {
        this.colType = columnType;
    }

    @Override
    public void update(DataBox value) {
        if (this.first == null) this.first = value;
    }

    @Override
    public DataBox getResult() {
        return this.first;
    }

    @Override
    public Type getResultType() {
        return colType;
    }

    @Override
    public void reset() {
        this.first = null;
    }
}
