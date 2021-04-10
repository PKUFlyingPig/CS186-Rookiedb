package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class UndoUpdatePageLogRecord extends LogRecord {
    private long transNum;
    private long pageNum;
    private long prevLSN;
    private long undoNextLSN;
    public short offset;
    public byte[] after;

    public UndoUpdatePageLogRecord(long transNum, long pageNum, long prevLSN, long undoNextLSN, short offset,
                            byte[] after) {
        super(LogType.UNDO_UPDATE_PAGE);
        this.transNum = transNum;
        this.pageNum = pageNum;
        this.prevLSN = prevLSN;
        this.undoNextLSN = undoNextLSN;
        this.offset = offset;
        this.after = after;
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
    public Optional<Long> getPageNum() {
        return Optional.of(pageNum);
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
        super.redo(rm, dsm, bm);

        Page page = bm.fetchPage(new DummyLockContext("_dummyUndoUpdatePageRecord"), pageNum);
        try {
            page.getBuffer().position(offset).put(after);
            page.setPageLSN(getLSN());
        } finally {
            page.unpin();
        }
        rm.dirtyPage(pageNum, getLSN());
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[(after.length == BufferManager.EFFECTIVE_PAGE_SIZE ? 36 : 37) + after.length];
        Buffer buf = ByteBuffer.wrap(b)
                     .put((byte) getType().getValue())
                     .putLong(transNum)
                     .putLong(pageNum)
                     .putLong(prevLSN)
                     .putLong(undoNextLSN)
                     .putShort(offset);
        // to make sure that the CLR can actually fit on one page...
        if (after.length == BufferManager.EFFECTIVE_PAGE_SIZE) {
            buf.put((byte) - 1).put(after);
        } else {
            buf.putShort((short) after.length).put(after);
        }
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        long transNum = buf.getLong();
        long pageNum = buf.getLong();
        long prevLSN = buf.getLong();
        long undoNextLSN = buf.getLong();
        short offset = buf.getShort();
        short length = buf.getShort();
        if (length < 0) {
            length = BufferManager.EFFECTIVE_PAGE_SIZE;
            buf.position(buf.position() - 1);
        }
        byte[] after = new byte[length];
        buf.get(after);
        return Optional.of(new UndoUpdatePageLogRecord(transNum, pageNum, prevLSN, undoNextLSN, offset,
                           after));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        UndoUpdatePageLogRecord that = (UndoUpdatePageLogRecord) o;
        return transNum == that.transNum &&
               pageNum == that.pageNum &&
               offset == that.offset &&
               prevLSN == that.prevLSN &&
               undoNextLSN == that.undoNextLSN &&
               Arrays.equals(after, that.after);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), transNum, pageNum, offset, prevLSN, undoNextLSN);
        result = 31 * result + Arrays.hashCode(after);
        return result;
    }

    @Override
    public String toString() {
        return "UndoUpdatePageLogRecord{" +
               "transNum=" + transNum +
               ", pageNum=" + pageNum +
               ", prevLSN=" + prevLSN +
               ", undoNextLSN=" + undoNextLSN +
               ", offset=" + offset +
               ", after=" + Arrays.toString(after) +
               ", LSN=" + LSN +
               '}';
    }
}
