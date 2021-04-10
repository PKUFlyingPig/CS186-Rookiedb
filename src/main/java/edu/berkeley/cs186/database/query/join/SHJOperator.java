package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.disk.Partition;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class SHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    /**
     * This class represents a simple hash join. To join the two relations the
     * class will attempt a single partitioning phase of the left records and
     * then probe with all of the right records. It will fail if any of the
     * partitions are larger than the B-2 pages of memory needed to construct
     * the in memory hash table by throwing an IllegalArgumentException.
     */
    public SHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // Since this has a chance of failing on certain inputs we give it the
        // maximum possible cost to encourage the optimizer to avoid it
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // Accumulate all of our joined records in this run and return an
            // iterator over it once the algorithm completes
            this.joinedRecords = new Run(getTransaction(), getSchema());
            this.run(getLeftSource(), getRightSource(), 1);
        };
        return joinedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Partition stage. For every record in the left record iterator, hashes the
     * value we are joining on and adds that record to the correct partition.
     */
    private void partition(Partition[] partitions, Iterable<Record> leftRecords) {
        for (Record record: leftRecords) {
            // Partition left records on the chosen column
            DataBox columnValue = record.getValue(getLeftColumnIndex());
            int hash = HashFunc.hashDataBox(columnValue, 1);
            // modulo to get which partition to use
            int partitionNum = hash % partitions.length;
            if (partitionNum < 0)  // hash might be negative
                partitionNum += partitions.length;
            partitions[partitionNum].add(record);
        }
    }

    /**
     * Builds the hash table using leftRecords and probes it with the records
     * in rightRecords. Joins the matching records and returns them as the
     * joinedRecords list.
     *
     * @param partition a partition
     * @param rightRecords An iterable of records from the right relation
     */
    private void buildAndProbe(Partition partition, Iterable<Record> rightRecords) {
        if (partition.getNumPages() > this.numBuffers - 2) {
            throw new IllegalArgumentException(
                    "The records in this partition cannot fit in B-2 pages of memory."
            );
        }

        // Our hash table to build on. The list contains all the records in the
        // left records that hash to the same key
        Map<DataBox, List<Record>> hashTable = new HashMap<>();

        // Building stage
        for (Record leftRecord: partition) {
            DataBox leftJoinValue = leftRecord.getValue(this.getLeftColumnIndex());
            if (!hashTable.containsKey(leftJoinValue)) {
                hashTable.put(leftJoinValue, new ArrayList<>());
            }
            hashTable.get(leftJoinValue).add(leftRecord);
        }

        // Probing stage
        for (Record rightRecord: rightRecords) {
            DataBox rightJoinValue = rightRecord.getValue(getRightColumnIndex());
            if (!hashTable.containsKey(rightJoinValue)) continue;
            // We have to join the right record with each left record with
            // a matching key
            for (Record lRecord : hashTable.get(rightJoinValue)) {
                Record joinedRecord = lRecord.concat(rightRecord);
                // Accumulate joined records in this.joinedRecords
                this.joinedRecords.add(joinedRecord);
            }
        }
    }

    /**
     * Runs the simple hash join algorithm. First, run the partitioning stage to
     * create an array of partitions. Then, build and probe with each hash
     * partitions records.
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("Reached the max number of passes");

        // Create empty partitions
        Partition[] partitions = createPartitions();

        // Partition records into left and right
        this.partition(partitions, leftRecords);

        for (int i = 0; i < partitions.length; i++) {
            buildAndProbe(partitions[i], rightRecords);
        }
    }

    /**
     * Create an appropriate number of partitions relative to the number of
     * available buffers we have and return an array
     *
     * @return an array of Partitions
     */
    private Partition[] createPartitions() {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            Schema schema = getLeftSource().getSchema();
            partitions[i] = new Partition(getTransaction(), schema);
        }
        return partitions;
    }
}

