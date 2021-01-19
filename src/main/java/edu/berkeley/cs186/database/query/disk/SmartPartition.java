package edu.berkeley.cs186.database.query.disk;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

/**
 * An extended version of the partition class that keeps track of whether all
 * the contained records are equal on a given column. Useful for determining
 * when to give up on recursive partitioning.
 */
public class SmartPartition extends Partition {
    private int hashColumnIndex;
    private boolean uniform = true;
    // TODO(proj3_part1): add any extra fields you like

    public SmartPartition(TransactionContext transaction, Schema s, int hashColumnIndex) {
        super(transaction, s);
        this.hashColumnIndex = hashColumnIndex;
        // TODO(proj3_part1): add any extra initialization you need
    }

    @Override
    public void add(Record record) {
        super.add(record);
        // TODO(proj3_part1): extend the functionality of add to track if the partition is uniform
    }

    /**
     * @return true if every record in this partition is equal on the hash
     * column.
     */
    @Override
    public boolean isUniform() {
        return uniform;
    }
}
