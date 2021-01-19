package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class UpdatePageLogRecord extends LogRecord {
    private long transNum;
    private long pageNum;
    private long prevLSN;
    public short offset;
    public byte[] before;
    public byte[] after;

    public UpdatePageLogRecord(long transNum, long pageNum, long prevLSN, short offset, byte[] before,
                        byte[] after) {
        super(LogType.UPDATE_PAGE);
        this.transNum = transNum;
        this.pageNum = pageNum;
        this.prevLSN = prevLSN;
        this.offset = offset;
        this.before = before == null ? new byte[0] : before;
        this.after = after == null ? new byte[0] : after;
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
    public boolean isUndoable() {
        return before.length > 0;
    }

    @Override
    public boolean isRedoable() {
        return after.length > 0;
    }

    @Override
    public Pair<LogRecord, Boolean> undo(long lastLSN) {
        if (!isUndoable()) {
            throw new UnsupportedOperationException("cannot undo this record: " + this);
        }
        return new Pair<>(new UndoUpdatePageLogRecord(transNum, pageNum, lastLSN, prevLSN, offset, before),
                          false);
    }

    @Override
    public void redo(DiskSpaceManager dsm, BufferManager bm) {
        super.redo(dsm, bm);

        Page page = bm.fetchPage(new DummyLockContext(), pageNum);
        try {
            page.getBuffer().position(offset).put(after);
            page.setPageLSN(getLSN());
        } finally {
            page.unpin();
        }
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[31 + before.length + after.length];
        ByteBuffer.wrap(b)
        .put((byte) getType().getValue())
        .putLong(transNum)
        .putLong(pageNum)
        .putLong(prevLSN)
        .putShort(offset)
        .putShort((short) before.length)
        .putShort((short) after.length)
        .put(before)
        .put(after);
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        long transNum = buf.getLong();
        long pageNum = buf.getLong();
        long prevLSN = buf.getLong();
        short offset = buf.getShort();
        byte[] before = new byte[buf.getShort()];
        byte[] after = new byte[buf.getShort()];
        buf.get(before).get(after);
        return Optional.of(new UpdatePageLogRecord(transNum, pageNum, prevLSN, offset, before, after));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        UpdatePageLogRecord that = (UpdatePageLogRecord) o;
        return transNum == that.transNum &&
               pageNum == that.pageNum &&
               offset == that.offset &&
               prevLSN == that.prevLSN &&
               Arrays.equals(before, that.before) &&
               Arrays.equals(after, that.after);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), transNum, pageNum, offset, prevLSN);
        result = 31 * result + Arrays.hashCode(before);
        result = 31 * result + Arrays.hashCode(after);
        return result;
    }

    @Override
    public String toString() {
        return "UpdatePageLogRecord{" +
               "transNum=" + transNum +
               ", pageNum=" + pageNum +
               ", offset=" + offset +
               ", before=" + Arrays.toString(before) +
               ", after=" + Arrays.toString(after) +
               ", prevLSN=" + prevLSN +
               ", LSN=" + LSN +
               '}';
    }
}
