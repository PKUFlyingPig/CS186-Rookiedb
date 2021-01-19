package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;

import java.util.*;

public class EndCheckpointLogRecord extends LogRecord {
    private Map<Long, Long> dirtyPageTable;
    private Map<Long, Pair<Transaction.Status, Long>> transactionTable;
    private Map<Long, List<Long>> touchedPages;
    private int numTouchedPages;

    public EndCheckpointLogRecord(Map<Long, Long> dirtyPageTable,
                                  Map<Long, Pair<Transaction.Status, Long>> transactionTable,
                                  Map<Long, List<Long>> touchedPages) {
        super(LogType.END_CHECKPOINT);
        this.dirtyPageTable = new HashMap<>(dirtyPageTable);
        this.transactionTable = new HashMap<>(transactionTable);
        this.touchedPages = new HashMap<>(touchedPages);
        this.numTouchedPages = 0;
        for (List<Long> pages : touchedPages.values()) {
            this.numTouchedPages += pages.size();
        }
    }

    @Override
    public Map<Long, Long> getDirtyPageTable() {
        return dirtyPageTable;
    }

    @Override
    public Map<Long, Pair<Transaction.Status, Long>> getTransactionTable() {
        return transactionTable;
    }

    @Override
    public Map<Long, List<Long>> getTransactionTouchedPages() {
        return touchedPages;
    }

    @Override
    public byte[] toBytes() {
        int recordSize = getRecordSize(dirtyPageTable.size(), transactionTable.size(), touchedPages.size(),
                                       numTouchedPages);
        byte[] b = new byte[recordSize];
        Buffer buf = ByteBuffer.wrap(b)
                     .put((byte) getType().getValue())
                     .putShort((short) dirtyPageTable.size())
                     .putShort((short) transactionTable.size())
                     .putShort((short) touchedPages.size());
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            buf.putLong(entry.getKey()).putLong(entry.getValue());
        }
        for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : transactionTable.entrySet()) {
            buf.putLong(entry.getKey())
            .put((byte) entry.getValue().getFirst().ordinal())
            .putLong(entry.getValue().getSecond());
        }
        for (Map.Entry<Long, List<Long>> entry : touchedPages.entrySet()) {
            buf.putLong(entry.getKey())
            .putShort((short) entry.getValue().size());
            for (Long pageNum : entry.getValue()) {
                buf.putLong(pageNum);
            }
        }
        return b;
    }

    /**
     * @return size of the record in bytes
     */
    public static int getRecordSize(int numDPTRecords, int numTxnTableRecords,
                                    int touchedPagesMapSize, int numTouchedPages) {
        // DPT: long -> long (16 bytes)
        // xact: long -> (byte, long) (17 bytes)
        // touched pages: long -> (short (size) + longs) (10 bytes + 8 bytes per page)
        return 7 + 16 * numDPTRecords + 17 * numTxnTableRecords + 10 * touchedPagesMapSize + 8 *
               numTouchedPages;
    }

    /**
     * @return boolean indicating whether information for
     * the log record can fit in one record on a page
     */
    public static boolean fitsInOneRecord(int numDPTRecords, int numTxnTableRecords,
                                          int touchedPagesMapSize, int numTouchedPages) {
        int recordSize = getRecordSize(numDPTRecords, numTxnTableRecords, touchedPagesMapSize,
                                       numTouchedPages);
        return recordSize <= DiskSpaceManager.PAGE_SIZE;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        short dptSize = buf.getShort();
        short xactSize = buf.getShort();
        short tpSize = buf.getShort();
        Map<Long, Long> dirtyPageTable = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> transactionTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        for (short i = 0; i < dptSize; ++i) {
            dirtyPageTable.put(buf.getLong(), buf.getLong());
        }
        for (short i = 0; i < xactSize; ++i) {
            long transNum = buf.getLong();
            Transaction.Status status = Transaction.Status.fromInt(buf.get());
            long lastLSN = buf.getLong();
            transactionTable.put(transNum, new Pair<>(status, lastLSN));
        }
        for (short i = 0; i < tpSize; ++i) {
            long transNum = buf.getLong();
            short numPages = buf.getShort();
            List<Long> pages = new ArrayList<>();
            for (short j = 0; j < numPages; ++j) {
                pages.add(buf.getLong());
            }
            touchedPages.put(transNum, pages);
        }
        return Optional.of(new EndCheckpointLogRecord(dirtyPageTable, transactionTable, touchedPages));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        EndCheckpointLogRecord that = (EndCheckpointLogRecord) o;
        return dirtyPageTable.equals(that.dirtyPageTable) &&
               transactionTable.equals(that.transactionTable) &&
               touchedPages.equals(that.touchedPages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dirtyPageTable, transactionTable, touchedPages);
    }

    @Override
    public String toString() {
        return "EndCheckpointLogRecord{" +
               "dirtyPageTable=" + dirtyPageTable +
               ", transactionTable=" + transactionTable +
               ", touchedPages=" + touchedPages +
               ", LSN=" + LSN +
               '}';
    }
}
