package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TestSourceOperator extends QueryOperator {
    private List<Record> records;
    private String sortedOn;

    public TestSourceOperator(List<Record> records, Schema schema) {
        super(OperatorType.SEQ_SCAN);
        this.records = records;
        this.setOutputSchema(schema);
        this.stats = this.estimateStats();
    }

    public TestSourceOperator(Record[] records, Schema schema) {
        this(Arrays.asList(records), schema);
    }

    /**
     * Constructor for empty source operator
     * @param schema schema for this source operator
     */
    public TestSourceOperator(Schema schema) {
        this(Collections.emptyList(), schema);
    }

    @Override
    public boolean isSequentialScan() {
        // We initialize ourselves with OperatorType SEQSCAN, but we technically
        // shouldn't be treated identically as one.
        return false;
    }

    public void setSortedOn(String s) {
        this.sortedOn = s;
    }

    @Override
    public List<String> sortedBy() {
        if (this.sortedOn == null) return Collections.emptyList();
        return Collections.singletonList(this.sortedOn);
    }

    @Override
    public Iterator<Record> iterator() {
        return this.records.iterator();
    }

    @Override
    protected Schema computeSchema() {
        return this.outputSchema;
    }

    @Override
    public TableStats estimateStats() {
        Schema schema = this.computeSchema();
        int recordsPerPage = Table.computeNumRecordsPerPage(
                BufferManager.EFFECTIVE_PAGE_SIZE,
                schema
        );
        return new TableStats(schema, recordsPerPage);
    }

    @Override
    public String str() {
        return "TestSourceOperator";
    }

    @Override
    public int estimateIOCost() {
        return 1;
    }
}
