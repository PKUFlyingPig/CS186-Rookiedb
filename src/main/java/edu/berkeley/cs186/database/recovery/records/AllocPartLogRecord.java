package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.util.Objects;
import java.util.Optional;

/**
 * A log entry that records the allocation of a partition
 */
public class AllocPartLogRecord extends LogRecord {
    private long transNum;
    private int partNum;
    private long prevLSN;

    public AllocPartLogRecord(long transNum, int partNum, long prevLSN) {
        super(LogType.ALLOC_PART);
        this.transNum = transNum;
        this.partNum = partNum;
        this.prevLSN = prevLSN;
    }

    @Override
    public Optional<Long> getTransNum() {
        return Optional.of(transNum);
    }

    @Override
    public Optional<Long> getPrevLSN() {
        return Optional.of(prevLSN);
    }

    @Override
    public Optional<Integer> getPartNum() {
        return Optional.of(partNum);
    }

    @Override
    public boolean isUndoable() {
        return true;
    }

    @Override
    public boolean isRedoable() {
        return true;
    }

    @Override
    public LogRecord undo(long lastLSN) {
        return new UndoAllocPartLogRecord(transNum, partNum, lastLSN, prevLSN);
    }

    @Override
    public void redo(RecoveryManager rm, DiskSpaceManager dsm, BufferManager bm) {
        super.redo(rm, dsm, bm);

        try {
            dsm.allocPart(partNum);
        } catch (IllegalStateException e) {
            /* do nothing - partition already exists */
        }
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[1 + Long.BYTES + Integer.BYTES + Long.BYTES];
        ByteBuffer.wrap(b)
        .put((byte) getType().getValue())
        .putLong(transNum)
        .putInt(partNum)
        .putLong(prevLSN);
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        long transNum = buf.getLong();
        int partNum = buf.getInt();
        long prevLSN = buf.getLong();
        return Optional.of(new AllocPartLogRecord(transNum, partNum, prevLSN));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        AllocPartLogRecord that = (AllocPartLogRecord) o;
        return transNum == that.transNum &&
               partNum == that.partNum &&
               prevLSN == that.prevLSN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), transNum, partNum, prevLSN);
    }

    @Override
    public String toString() {
        return "AllocPartLogRecord{" +
               "transNum=" + transNum +
               ", partNum=" + partNum +
               ", prevLSN=" + prevLSN +
               ", LSN=" + LSN +
               '}';
    }
}
