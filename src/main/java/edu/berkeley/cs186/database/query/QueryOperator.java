package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.table.PageDirectory;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

public abstract class QueryOperator implements Iterable<Record> {
    protected QueryOperator source;
    protected Schema outputSchema;
    protected TableStats stats;

    public enum OperatorType {
        PROJECT,
        SEQ_SCAN,
        INDEX_SCAN,
        JOIN,
        SELECT,
        GROUP_BY,
        SORT,
        LIMIT,
        MATERIALIZE
    }

    private OperatorType type;

    /**
     * Creates a QueryOperator without a set source, destination, or schema.
     * @param type the operator's type (Join, Project, Select, etc...)
     */
    public QueryOperator(OperatorType type) {
        this.type = type;
        this.source = null;
        this.outputSchema = null;
    }

    /**
     * Creates a QueryOperator with a set source, and computes the output schema
     * accordingly.
     * @param type the operator's type (Join, Project, Select, etc...)
     * @param source the source operator
     */
    protected QueryOperator(OperatorType type, QueryOperator source) {
        this.source = source;
        this.type = type;
        this.outputSchema = this.computeSchema();
    }

    /**
     * @return an enum value representing the type of this operator (Join,
     * Project, Select, etc...)
     */
    public OperatorType getType() {
        return this.type;
    }

    /**
     * @return True if this operator is a join operator, false otherwise.
     */
    public boolean isJoin() {
        return this.type.equals(OperatorType.JOIN);
    }

    /**
     * @return True if this operator is a select operator, false otherwise.
     */
    public boolean isSelect() {
        return this.type.equals(OperatorType.SELECT);
    }

    /**
     * @return True if this operator is a project operator, false otherwise.
     */
    public boolean isProject() {
        return this.type.equals(OperatorType.PROJECT);
    }

    /**
     * @return True if this operator is a group by operator, false otherwise.
     */
    public boolean isGroupBy() {
        return this.type.equals(OperatorType.GROUP_BY);
    }

    /**
     * @return True if this operator is a sequential scan operator, false otherwise.
     */
    public boolean isSequentialScan() {
        return this.type.equals(OperatorType.SEQ_SCAN);
    }

    /**
     * @return True if this operator is an index scan operator, false otherwise.
     */
    public boolean isIndexScan() {
        return this.type.equals(OperatorType.INDEX_SCAN);
    }

    public List<String> sortedBy() {
        return Collections.emptyList();
    }

    /**
     * @return the source operator from which this operator draws records from
     */
    public QueryOperator getSource() {
        return this.source;
    }

    /**
     * Sets the source of this operator and uses it to compute the output schema
     */
    protected void setSource(QueryOperator source) {
        this.source = source;
        this.outputSchema = this.computeSchema();
    }

    /**
     * @return the schema of the records obtained when executing this operator
     */
    public Schema getSchema() {
        return this.outputSchema;
    }

    /**
     * Sets the output schema of this operator. This should match the schema of the records of the iterator obtained
     * by calling execute().
     */
    protected void setOutputSchema(Schema schema) {
        this.outputSchema = schema;
    }

    /**
     * Computes the schema of this operator's output records.
     * @return a schema matching the schema of the records of the iterator
     * obtained by calling .iterator()
     */
    protected abstract Schema computeSchema();

    /**
     * @return an iterator over the output records of this operator
     */
    public abstract Iterator<Record> iterator();

    /**
     * @return true if the records of this query operator are materialized in a
     * table.
     */
    public boolean materialized() {
        return false;
    }

    public BacktrackingIterator<Record> backtrackingIterator() {
        throw new UnsupportedOperationException(
            "This operator doesn't support backtracking. You may want to " +
            "use QueryOperator.materialize on it first."
        );
    }

    /**
     * Utility method that checks to see if a column is found in a schema using
     * dot notation.
     *
     * @param fromSchema the schema to search in
     * @param specified the column name to search for
     * @return
     */
    public static boolean checkColumnNameEquality(String fromSchema, String specified) {
        if (fromSchema.equals(specified)) {
            return true;
        }
        if (!specified.contains(".")) {
            String schemaColName = fromSchema;
            if (fromSchema.contains(".")) {
                String[] splits = fromSchema.split("\\.");
                schemaColName = splits[1];
            }

            return schemaColName.equals(specified);
        }
        return false;
    }

    /**
     * Utility method to determine whether or not a specified column name is
     * valid with a given schema.
     *
     * @param schema
     * @param columnName
     */
    public static String checkSchemaForColumn(Schema schema, String columnName) {
        List<String> schemaColumnNames = schema.getFieldNames();
        boolean found = false;
        String foundName = null;
        for (String sourceColumnName : schemaColumnNames) {
            if (checkColumnNameEquality(sourceColumnName, columnName)) {
                if (found) {
                    throw new RuntimeException("Column " + columnName + " specified twice without disambiguation.");
                }
                found = true;
                foundName = sourceColumnName;
            }
        }
        if (!found) {
            throw new RuntimeException("No column " + columnName + " found.");
        }
        return foundName;
    }

    /**
     * @return This method will consume up to `maxPages` pages of records from
     * `records` (advancing it in the process) and return a backtracking
     * iterator over those records.
     */
    public static BacktrackingIterator<Record> getBlockIterator(Iterator<Record> records, Schema schema, int maxPages) {
        int recordsPerPage = Table.computeNumRecordsPerPage(PageDirectory.EFFECTIVE_PAGE_SIZE, schema);
        int maxRecords = recordsPerPage * maxPages;
        List<Record> blockRecords = new ArrayList<>();
        for (int i = 0; i < maxRecords && records.hasNext(); i++) {
            blockRecords.add(records.next());
        }
        return new ArrayBacktrackingIterator<>(blockRecords);
    }

    public static QueryOperator materialize(QueryOperator operator, TransactionContext transaction) {
        if (!operator.materialized()) {
            return new MaterializeOperator(operator, transaction);
        }
        return operator;
    }

    public abstract String str();

    public String toString() {
        String r = this.str();
        if (this.source != null) {
            r += ("\n-> " + this.source.toString()).replaceAll("\n", "\n\t");
        }
        return r;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    public abstract TableStats estimateStats();

    /**
     * Estimates the IO cost of executing this query operator.
     *
     * @return estimated number of IO's performed
     */
    public abstract int estimateIOCost();

}
