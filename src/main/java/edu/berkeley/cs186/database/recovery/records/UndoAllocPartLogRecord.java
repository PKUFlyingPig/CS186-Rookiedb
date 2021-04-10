package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public class UndoAllocPartLogRecord extends LogRecord {
    private long transNum;
    private int partNum;
    private long prevLSN;
    private long undoNextLSN;

    public UndoAllocPartLogRecord(long transNum, int partNum, long prevLSN, long undoNextLSN) {
        super(LogType.UNDO_ALLOC_PART);
        this.transNum = transNum;
        this.partNum = partNum;
        this.prevLSN = prevLSN;
        this.undoNextLSN = undoNextLSN;
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
    public Optional<Long> getUndoNextLSN() {
        return Optional.of(undoNextLSN);
    }

    @Override
    public boolean isRedoable() {
        return true;
    }

    @Override
    public void redo(RecoveryManager rm, DiskSpaceManager dsm, BufferManager bm) {
        // Freed partition will disappear on disk after freePart is called,
        // so we must flush up to this record before calling it.
        rm.flushToLSN(getLSN());
        super.redo(rm, dsm, bm);
        try {
            dsm.freePart(partNum);
        } catch (NoSuchElementException e) {
            /* do nothing - partition already freed */
        }
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[1 + Long.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES];
        ByteBuffer.wrap(b)
        .put((byte) getType().getValue())
        .putLong(transNum)
        .putInt(partNum)
        .putLong(prevLSN)
        .putLong(undoNextLSN);
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        long transNum = buf.getLong();
        int partNum = buf.getInt();
        long prevLSN = buf.getLong();
        long undoNextLSN = buf.getLong();
        return Optional.of(new UndoAllocPartLogRecord(transNum, partNum, prevLSN, undoNextLSN));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        UndoAllocPartLogRecord that = (UndoAllocPartLogRecord) o;
        return transNum == that.transNum &&
               partNum == that.partNum &&
               prevLSN == that.prevLSN &&
               undoNextLSN == that.undoNextLSN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), transNum, partNum, prevLSN, undoNextLSN);
    }

    @Override
    public String toString() {
        return "UndoAllocPartLogRecord{" +
               "transNum=" + transNum +
               ", partNum=" + partNum +
               ", prevLSN=" + prevLSN +
               ", undoNextLSN=" + undoNextLSN +
               ", LSN=" + LSN +
               '}';
    }
}
