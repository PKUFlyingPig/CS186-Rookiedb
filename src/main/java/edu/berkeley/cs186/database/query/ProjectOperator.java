package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.cli.parser.ParseException;
import edu.berkeley.cs186.database.cli.parser.RookieParser;
import edu.berkeley.cs186.database.cli.visitor.ExpressionVisitor;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.aggr.DataFunction;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ProjectOperator extends QueryOperator {
    // A list of column names to use in the output of this operator
    private List<String> outputColumns;

    // The names of columns in the GROUP BY clause of this query.
    private List<String> groupByColumns;

    // Schema of the source operator
    private Schema sourceSchema;

    // List of DataFunctions
    private List<DataFunction> dataFunctions;

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
        List<DataFunction> dataFunctions = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            // No data functions provided, manually parse each input column
            RookieParser parser = new RookieParser(
                    new ByteArrayInputStream(columns.get(i).getBytes(StandardCharsets.UTF_8)));
            try {
                ExpressionVisitor visitor = new ExpressionVisitor();
                parser.expression().jjtAccept(visitor, null);
                dataFunctions.add(visitor.build());
            } catch (ParseException e) {
                throw new DatabaseException(e.getMessage());
            }
        }
        initialize(source, columns, dataFunctions, groupByColumns);
    }

    public ProjectOperator(QueryOperator source, List<String> columns, List<DataFunction> dataFunctions, List<String> groupByColumns) {
        super(OperatorType.PROJECT);
        initialize(source, columns, dataFunctions, groupByColumns);
    }

    public void initialize(QueryOperator source, List<String> columns, List<DataFunction> dataFunctions, List<String> groupByColumns) {
        this.outputColumns = columns;
        this.groupByColumns = groupByColumns;
        this.dataFunctions = dataFunctions;
        this.sourceSchema = source.getSchema();
        this.source = source;
        Schema schema = new Schema();
        for (int i = 0; i < columns.size(); i++) {
            dataFunctions.get(i).setSchema(this.sourceSchema);
            schema.add(columns.get(i), dataFunctions.get(i).getType());
        }
        this.outputSchema = schema;

        Set<Integer> groupByIndices = new HashSet<>();
        for (String colName: groupByColumns) {
            groupByIndices.add(this.sourceSchema.findField(colName));
        }
        boolean hasAgg = false;
        for (int i = 0; i < dataFunctions.size(); i++) {
            hasAgg |= dataFunctions.get(i).hasAgg();
        }
        if (!hasAgg) return;

        for (int i = 0; i < dataFunctions.size(); i++) {
            Set<Integer> dependencyIndices = new HashSet<>();
            for (String colName: dataFunctions.get(i).getDependencies()) {
                dependencyIndices.add(this.sourceSchema.findField(colName));
            }
            if (!dataFunctions.get(i).hasAgg()) {
                dependencyIndices.removeAll(groupByIndices);
                if (dependencyIndices.size() != 0) {
                    int any = dependencyIndices.iterator().next();
                    throw new UnsupportedOperationException(
                            "Non aggregate expression `" + columns.get(i) +
                                    "` refers to ungrouped field `" + sourceSchema.getFieldName(any) + "`"
                    );
                }
            }
        }
    }

    @Override
    public boolean isProject() { return true; }

    @Override
    protected Schema computeSchema() {
        return this.outputSchema;
    }

    @Override
    public Iterator<Record> iterator() {
        return new ProjectIterator();
    }

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

    private class ProjectIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private boolean hasAgg = false;

        private ProjectIterator() {
            this.sourceIterator = ProjectOperator.this.getSource().iterator();
            for (DataFunction func: dataFunctions) {
                this.hasAgg |= func.hasAgg();
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
            if (!this.hasAgg && groupByColumns.size() == 0 ) {
                List<DataBox> newValues = new ArrayList<>();
                for (DataFunction f: dataFunctions) {
                    newValues.add(f.evaluate(curr));
                }
                return new Record(newValues);
            }

            // Everything after here is to handle aggregation
            Record base = curr; // We'll draw the GROUP BY values from here
            while (curr != GroupByOperator.MARKER) {
                for (DataFunction dataFunction: dataFunctions) {
                    if (dataFunction.hasAgg()) dataFunction.update(curr);
                }
                if (!sourceIterator.hasNext()) break;
                curr = this.sourceIterator.next();
            }

            // Figure out where to get each value in the output record from
            List<DataBox> values = new ArrayList<>();
            for (DataFunction dataFunction: dataFunctions) {
                if (dataFunction.hasAgg()) {
                    values.add(dataFunction.evaluate(base));
                    dataFunction.reset();
                } else {
                    values.add(dataFunction.evaluate(base));
                }
            }
            return new Record(values);
        }
    }
}
