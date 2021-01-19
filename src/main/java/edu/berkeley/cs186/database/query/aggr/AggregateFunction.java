package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aggregate functions accept values and update internal state. The internal
 * state can then be used to compute a result when getResult() is called. The
 * internal state can be reset by calling reset().
 *
 * To help compute output schemas AggregateFunctions must also provide the
 * TypeId of their output.
 */
public abstract class AggregateFunction {
    /**
     * Updates internal state of the aggregate based on the value
     */
    public abstract void update(DataBox value);

    /**
     * Computes the result of the aggregate based on internal state
     */
    public abstract DataBox getResult();

    /**
     * Returns the TypeId of the result
     */
    public abstract Type getResultType();

    /**
     * Resets the internal state of the aggregate function
     */
    public abstract void reset();

    /**
     * Takes the name of an aggregate function and a type id for the column the
     * aggregate will be computed on. Returns an appropriate aggregate function.
     */
    public static AggregateFunction getAggregate(String name, Type columnType) {
        name = name.toUpperCase().trim();
        switch (name) {
            case "FIRST": return new FirstAggregateFunction(columnType);
            case "SUM": return new SumAggregateFunction(columnType);
            case "COUNT": return new CountAggregateFunction();
            case "MAX": return new MaxAggregateFunction(columnType);
            case "MIN": return new MinAggregateFunction(columnType);
            case "AVG": return new AverageAggregateFunction(columnType);
            case "VARIANCE": return new VarianceAggregateFunction(columnType);
            case "STDDEV": return new StdDevAggregateFunction(columnType);
            case "RANGE": return new RangeAggregateFunction(columnType);
            case "RANDOM": return new RandomAggregateFunction(columnType);
            case "LAST": return new LastAggregateFunction(columnType);
        }
        throw new IllegalArgumentException("Unrecognized aggregate: " + name);
    }

    /**
     * Takes a column name and determines if it contains an aggregate with a
     * regex match.
     * @return an array containing the name of the aggregate the name of the
     * column the aggregate is being performed on, otherwise null if no
     * aggregate was found.
     */
    public static String[] splitAggregate(String columnName) {
        Pattern pat = Pattern.compile("^\\s*(.+)\\s*[(]\\s*(.+)\\s*[)]\\s*$");
        Matcher mat = pat.matcher(columnName);
        if (mat.find()) {
            return new String[]{mat.group(1), mat.group(2)};
        }
        return null;
    }

}
