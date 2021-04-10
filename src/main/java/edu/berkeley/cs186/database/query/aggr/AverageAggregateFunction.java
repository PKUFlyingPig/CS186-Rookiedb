package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.databox.TypeId;

/**
 * An AVG aggregate keeps a cumulative sum of the values it has seen and returns
 * that sum over the total number of values. Undefined for the STRING data
 * type. Always returns FLOAT type result.
 */
public class AverageAggregateFunction extends AggregateFunction {
    private SumAggregateFunction sumAgg;
    float count = 0;

    public AverageAggregateFunction(Type colType) {
        if (colType.getTypeId() == TypeId.STRING) {
            throw new IllegalArgumentException("Invalid data type for AVG aggregate: STRING");
        }
        this.sumAgg = new SumAggregateFunction(colType);
    }

    @Override
    public void update(DataBox d) {
        this.sumAgg.update(d);
        count++;
    }

    @Override
    public DataBox getResult() {
        DataBox sum = this.sumAgg.getResult();
        if (count == 0) return new FloatDataBox(Float.NEGATIVE_INFINITY);
        switch (sum.getTypeId()) {
            case INT: return new FloatDataBox(sum.getInt() / count);
            case LONG: return new FloatDataBox(sum.getLong() / count);
            case FLOAT: return new FloatDataBox(sum.getFloat() / count);
        }
        throw new IllegalStateException("Unreachable code.");
    }

    @Override
    public Type getResultType() {
        return Type.floatType();
    }

    @Override
    public void reset() {
        this.count = 0;
        sumAgg.reset();
    }
}
