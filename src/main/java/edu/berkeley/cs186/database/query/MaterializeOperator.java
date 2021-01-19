package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.table.Record;

import java.util.List;

public class MaterializeOperator extends SequentialScanOperator {
    /**
     * Operator that materializes the source operator into a temporary table immediately,
     * and then acts as a sequential scan operator over the temporary table.
     * @param source source operator to be materialized
     * @param transaction current running transaction
     */
    public MaterializeOperator(QueryOperator source,
                        TransactionContext transaction) {
        super(OperatorType.MATERIALIZE, transaction, materializeToTable(source, transaction));
        setSource(source);
        setOutputSchema(source.getSchema());
    }

    private static String materializeToTable(QueryOperator source, TransactionContext transaction) {
        String materializedTableName = transaction.createTempTable(source.getSchema());
        for (Record record : source) {
            transaction.addRecord(materializedTableName, record);
        }
        return materializedTableName;
    }

    @Override
    public String str() {
        return "Materialize (cost: " + this.estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return getSource().sortedBy();
    }
}
