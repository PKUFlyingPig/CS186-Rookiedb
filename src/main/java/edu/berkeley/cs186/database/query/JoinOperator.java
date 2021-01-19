package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

public abstract class JoinOperator extends QueryOperator {
    public enum JoinType {
        SNLJ,
        PNLJ,
        BNLJ,
        SORTMERGE,
        SHJ,
        GHJ
    }
    protected JoinType joinType;

    // the source operators
    private QueryOperator leftSource;
    private QueryOperator rightSource;

    // join column indices
    private int leftColumnIndex;
    private int rightColumnIndex;

    // join column names
    private String leftColumnName;
    private String rightColumnName;

    // current transaction
    private TransactionContext transaction;

    /**
     * Create a join operator that pulls tuples from leftSource and rightSource. Returns tuples for which
     * leftColumnName and rightColumnName are equal.
     *
     * @param leftSource the left source operator
     * @param rightSource the right source operator
     * @param leftColumnName the column to join on from leftSource
     * @param rightColumnName the column to join on from rightSource
     */
    public JoinOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction,
                 JoinType joinType) {
        super(OperatorType.JOIN);
        this.joinType = joinType;
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.leftColumnName = leftColumnName;
        this.rightColumnName = rightColumnName;
        this.setOutputSchema(this.computeSchema());
        this.transaction = transaction;
    }

    @Override
    public QueryOperator getSource() {
        throw new RuntimeException("There is no single source for join operators. use " +
                                     "getRightSource and getLeftSource and the corresponding set methods.");
    }

    @Override
    public Schema computeSchema() {
        // Get lists of the field names of the records
        Schema leftSchema = this.leftSource.getSchema();
        Schema rightSchema = this.rightSource.getSchema();
        List<String> leftSchemaNames = new ArrayList<>(leftSchema.getFieldNames());
        List<String> rightSchemaNames = new ArrayList<>(rightSchema.getFieldNames());

        // Set up join column attributes
        this.leftColumnName = this.checkSchemaForColumn(leftSchema, this.leftColumnName);
        this.leftColumnIndex = leftSchemaNames.indexOf(leftColumnName);
        this.rightColumnName = this.checkSchemaForColumn(rightSchema, this.rightColumnName);
        this.rightColumnIndex = rightSchemaNames.indexOf(rightColumnName);

        // Check that the types of the columns of each input operator match
        List<Type> leftSchemaTypes = new ArrayList<>(leftSchema.getFieldTypes());
        List<Type> rightSchemaTypes = new ArrayList<>(rightSchema.getFieldTypes());
        if (!leftSchemaTypes.get(this.leftColumnIndex).getClass().equals(rightSchemaTypes.get(
                this.rightColumnIndex).getClass())) {
            throw new RuntimeException("Mismatched types of columns " + leftColumnName + " and "
                    + rightColumnName + ".");
        }

        // Return concatenated schema
        return leftSchema.concat(rightSchema);
    }

    @Override
    public String str() {
        return String.format("%s on %s=%s (cost=%d)",
                this.joinType, this.leftColumnName, this.rightColumnName,
                this.estimateIOCost());
    }

    @Override
    public String toString() {
        String r = this.str();
        if (this.leftSource != null) {
            r += ("\n-> " + this.leftSource.toString()).replaceAll("\n", "\n\t");
        }
        if (this.rightSource != null) {
            r += ("\n-> " + this.rightSource.toString()).replaceAll("\n", "\n\t");
        }
        return r;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    @Override
    public TableStats estimateStats() {
        TableStats leftStats = this.leftSource.estimateStats();
        TableStats rightStats = this.rightSource.estimateStats();
        return leftStats.copyWithJoin(this.leftColumnIndex,
                rightStats,
                this.rightColumnIndex);
    }

    /**
     * @return the query operator which supplies the left records of the join
     */
    protected QueryOperator getLeftSource() {
        return this.leftSource;
    }

    /**
     * @return the query operator which supplies the right records of the join
     */
    protected QueryOperator getRightSource() {
        return this.rightSource;
    }

    /**
     * @return the transaction context this operator is being executed within
     */
    public TransactionContext getTransaction() {
        return this.transaction;
    }

    /**
     * @return the name of the left column being joined on
     */
    public String getLeftColumnName() {
        return this.leftColumnName;
    }

    /**
     * @return the name of the right column being joined on
     */
    public String getRightColumnName() {
        return this.rightColumnName;
    }

    /**
     * @return the position of the column being joined on in the left relation's schema. Can be used to determine which
     * value in the left relation's records to check for equality on.
     */
    public int getLeftColumnIndex() {
        return this.leftColumnIndex;
    }

    /**
     * @return the position of the column being joined on in the right relation's schema. Can be used to determine which
     * value in the right relation's records to check for equality on.
     */
    public int getRightColumnIndex() {
        return this.rightColumnIndex;
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * @return 0 if leftRecord and rightRecord match on their join values, -1 if leftRecord's join value is less
     * than rightRecord's join value, 1 if leftRecord's join value is greater than rightRecord's join value.
     */
    public int compare(Record leftRecord, Record rightRecord) {
        DataBox leftRecordValue = leftRecord.getValue(this.leftColumnIndex);
        DataBox rightRecordValue = rightRecord.getValue(this.rightColumnIndex);
        return leftRecordValue.compareTo(rightRecordValue);
    }
}
