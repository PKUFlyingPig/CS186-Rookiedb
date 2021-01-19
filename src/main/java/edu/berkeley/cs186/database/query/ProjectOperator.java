package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.aggr.AggregateFunction;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class ProjectOperator extends QueryOperator {
    // Magic number for the source index of aggregates on *, e.g. COUNT(*)
    private final static int STAR_INDEX = -0xDecafBad;

    // A list of column names to use in the output of this operator
    private List<String> outputColumns;

    // Each index in this list corresponds to a column name in the output. The
    // index represents which position in the source operator the output column
    // should draw its values from. Negative indices indicate that an aggregate
    // is being performed with the output column, and to draw values from the
    // AggregateFunction at the position (-1 * index - 1) of this.aggregates.
    private List<Integer> sourceIndices;

    // The names of columns in the GROUP BY clause of this query.
    private List<String> groupByColumns;

    // Pairs consisting of the name of an aggregate and the index in the source
    // operator's output that the aggregate should be computed on.
    private List<Pair<String, Integer>> aggregates;

    // Schema of the source operator
    private Schema sourceSchema;

    /**
     * Creates a new ProjectOperator that reads tuples from source and filters
     * out columns. Optionally computes an aggregate if it is specified.
     *
     * @param source
     * @param columns
     * @param groupByColumns
     */
    public ProjectOperator(QueryOperator source, List<String> columns, List<String> groupByColumns) {
        super(OperatorType.PROJECT);
        this.outputColumns = columns;
        this.groupByColumns = groupByColumns;
        this.sourceIndices = new ArrayList<>();
        this.aggregates = new ArrayList<>();
        this.sourceSchema = source.getSchema();

        // Don't need to explicitly set the output schema because setting the
        // source recomputes the schema.
        this.setSource(source);
        this.stats = this.estimateStats();
    }

    /**
     * @return true if this project involves a GROUP BY or aggregate functions,
     * false otherwise.
     */
    private boolean hasAggregate() {
        return this.groupByColumns.size() != 0 || this.aggregates.size() != 0;
    }

    /**
     * @param columnName
     * @return index in the source schema of the given column name
     */
    private int getSourceIndex(String columnName) {
        columnName = this.checkSchemaForColumn(sourceSchema, columnName);
        return this.sourceSchema.getFieldNames().indexOf(columnName);
    }

    /**
     * @param columnName
     * @return type in the source schema of the given column name
     */
    private Type getSourceType(String columnName) {
        int i = getSourceIndex(columnName);
        return this.sourceSchema.getFieldTypes().get(i);
    }

    @Override
    public boolean isProject() { return true; }

    @Override
    protected Schema computeSchema() {
        // Store source indices of columns in the GROUP BY clause
        ArrayList<Integer> groupByIndices = new ArrayList<>();
        for (String column: this.groupByColumns) {
            groupByIndices.add(getSourceIndex(column));
        }

        // Go through each column to separate out the aggregate functions,
        // determine which index in the source operator to draw values from,
        // and determine the type of the column in the output schema
        Schema s = new Schema();
        for (String column : this.outputColumns) {
            // Separate aggregate from column name if there was an aggregate
            String[] aggregateSplit = AggregateFunction.splitAggregate(column);
            if (aggregateSplit != null) column = aggregateSplit[1];

            Type outputType = null;
            int sourceIndex = STAR_INDEX;

            if (!column.equals("*")) { // Ignore * since it isn't a column
                sourceIndex = getSourceIndex(column);
                outputType = getSourceType(column);
            }

            if (aggregateSplit != null) {
                // Don't allow aggregate on columns that are part of GROUP BY
                if (groupByIndices.contains(sourceIndex)) {
                    throw new RuntimeException(String.format(
                            "Can't use column `%s` in both aggregate and GROUP BY clause.",
                            column
                    ));
                }
                // Create the aggregate, update type and index accordingly
                AggregateFunction function = AggregateFunction.getAggregate(
                        aggregateSplit[0],
                        outputType
                );
                // We have to store a string here instead of the function
                // so that multiple source iterators can be made without
                // interfering with each other.
                this.aggregates.add(new Pair<>(aggregateSplit[0], sourceIndex));
                outputType = function.getResultType();
                sourceIndex = -1 * aggregates.size();
            }
            s.add(column, outputType);
            this.sourceIndices.add(sourceIndex);
        }

        // If a GROUP BY clause or an aggregate function was used, ensure
        // that the source for every column in the output is either an aggregate
        // function or part of the GROUP BY clause
        if (this.hasAggregate()) {
            for (int i : this.sourceIndices) {
                if (i < 0 || groupByIndices.contains(i)) continue;
                throw new RuntimeException(String.format(
                    "Column `%s` must be part of an aggregate function " +
                    "or in the GROUP BY to be in the SELECT clause.",
                    this.sourceSchema.getFieldNames().get(i)
                ));
            }
        }
        return s;
    }

    @Override
    public Iterator<Record> iterator() { return new ProjectIterator(); }

    @Override
    public String str() {
        String columns = "(" + String.join(", ", this.outputColumns) + ")";
        return "Project (cost=" + this.estimateIOCost() + ")" +
               "\n\tcolumns: " + columns;
    }

    @Override
    public TableStats estimateStats() {
        return this.getSource().estimateStats();
    }

    @Override
    public int estimateIOCost() {
        return this.getSource().estimateIOCost();
    }

    /**
     * Iterator that transforms records from the source operator by projecting
     * out and rearranging columns. If an aggregate was specified, this iterator
     * handles the logic for executing aggregate functions.
     */
    private class ProjectIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private List<Pair<AggregateFunction, Integer>> aggFuncs;

        private ProjectIterator() {
            this.sourceIterator = ProjectOperator.this.getSource().iterator();
            this.aggFuncs = new ArrayList<>();
            for (Pair<String, Integer> pair: ProjectOperator.this.aggregates) {
                // Create (aggregate function, source index) pairs
                Type type = null;
                if (pair.getSecond() != STAR_INDEX) {
                    type = sourceSchema.getFieldTypes().get(pair.getSecond());
                }
                aggFuncs.add(new Pair<>(
                    AggregateFunction.getAggregate(pair.getFirst(), type),
                    pair.getSecond()
                ));
            }
        }

        @Override
        public boolean hasNext() {
            return this.sourceIterator.hasNext();
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record curr = this.sourceIterator.next();
            if (!hasAggregate()) {
                List<DataBox> recordValues = curr.getValues();
                List<DataBox> newValues = new ArrayList<>();
                for (int index : ProjectOperator.this.sourceIndices) {
                    newValues.add(recordValues.get(index));
                }
                return new Record(newValues);
            }

            // Everything after here is to handle aggregation
            Record base = curr; // We'll draw the GROUP BY values from here
            while (curr != GroupByOperator.MARKER) {
                for (Pair<AggregateFunction, Integer> pair: aggFuncs) {
                    // Until we run out of records or hit a marker, keep
                    // updating our aggregates with the appropriate values
                    AggregateFunction aggFunc = pair.getFirst();
                    int i = pair.getSecond();
                    DataBox value = i == STAR_INDEX ? null : curr.getValue(i);
                    aggFunc.update(value);
                }
                if (!sourceIterator.hasNext()) break;
                curr = this.sourceIterator.next();
            }

            // Figure out where to get each value in the output record from
            List<DataBox> values = new ArrayList<>();
            for (int i: ProjectOperator.this.sourceIndices) {
                if (i < 0) {
                    // Negative index means value comes from an aggregate
                    i = (-1 * i) - 1;
                    AggregateFunction aggFunc = aggFuncs.get(i).getFirst();
                    values.add(aggFunc.getResult());
                    aggFunc.reset();
                } else {
                    // Positive means value was part of GROUP BY, take from
                    // the source.
                    values.add(base.getValue(i));
                }
            }
            return new Record(values);
        }
    }
}
