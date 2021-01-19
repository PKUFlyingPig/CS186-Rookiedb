package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.*;

/**
 * A SUM aggregate keeps a cumulative sum of the values it has seen so far and
 * returns that sum as a result. Undefined for the STRING data type. If the
 * column type is BOOL or INT the result type will be INT. If the column type is
 * LONG the result type will be LONG. If the column type is FLOAT the result type
 * will be FLOAT.
 */
public class SumAggregateFunction extends AggregateFunction {
    private Type colType;
    private float floatSum = 0;
    private int intSum = 0;
    private long longSum = 0;
    public SumAggregateFunction(Type colType) {
        this.colType = colType;
        if (this.colType.getTypeId() == TypeId.STRING) {
            throw new IllegalArgumentException("Invalid data type for SUM aggregate: STRING");
        }
    }

    @Override
    public void update(DataBox d) {
        switch (d.getTypeId()) {
            case BOOL:
                boolean b = d.getBool();
                if (b) intSum++;
                return;
            case INT:
                int i = d.getInt();
                intSum += i;
                return;
            case LONG:
                long l = d.getLong();
                longSum += l;
                return;
            case FLOAT:
                float f = d.getFloat();
                floatSum += f;
                return;
        }
        throw new IllegalStateException("Unreachable code.");
    }

    @Override
    public DataBox getResult() {
        switch (getResultType().getTypeId()) {
            case INT: return new IntDataBox(intSum);
            case LONG: return new LongDataBox(longSum);
            case FLOAT: return new FloatDataBox(floatSum);
        }
        throw new IllegalStateException("Unreachable code.");
    }

    @Override
    public Type getResultType() {
        switch (this.colType.getTypeId()) {
            case BOOL:
            case INT: return Type.intType();
            case LONG: return Type.longType();
            case FLOAT: return Type.floatType();
        }
        throw new IllegalStateException("Unreachable code.");
    }

    @Override
    public void reset() {
        this.floatSum = 0;
        this.longSum = 0;
        this.intSum = 0;
    }
}
