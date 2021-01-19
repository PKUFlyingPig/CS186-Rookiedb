package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;

/**
 * A COUNT aggregate counts the number of values passed in and returns the total
 * as a result. Works for all data types and always returns an INT type result.
 */
public class CountAggregateFunction extends AggregateFunction {
    private int count = 0;
    @Override
    public void update(DataBox d) {
        count++;
    }

    @Override
    public DataBox getResult() {
        return new IntDataBox(count);
    }

    @Override
    public Type getResultType() {
        return Type.intType();
    }

    @Override
    public void reset() {
        this.count = 0;
    }
}