package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;

import java.util.Random;

/**
 * A RANDOM aggregate will uniformly at random return one of the values it has
 * been fed as a result. Works for all data types and always returns the same
 * data type as the column being aggregated.
 */
public class RandomAggregateFunction extends AggregateFunction {
    private Type colType;
    private int count = 0;
    private DataBox value;
    private Random generator;

    public RandomAggregateFunction(Type columnType) {
        this.colType = columnType;
        this.generator = new Random();
    }

    @Override
    public void update(DataBox value) {
        count += 1;
        if (generator.nextDouble() <= 1.0 / count) {
            this.value = value;
        }
    }

    @Override
    public DataBox getResult() {
        return this.value;
    }

    @Override
    public Type getResultType() {
        return this.colType;
    }

    @Override
    public void reset() {
        this.value = null;
        this.count = 0;
    }
}
