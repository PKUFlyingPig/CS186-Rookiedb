package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;

import java.util.Objects;
import java.util.Optional;

public class BeginCheckpointLogRecord extends LogRecord {
    public long maxTransNum;

    public BeginCheckpointLogRecord(long maxTransNum) {
        super(LogType.BEGIN_CHECKPOINT);
        this.maxTransNum = maxTransNum;
    }

    @Override
    public Optional<Long> getMaxTransactionNum() {
        return Optional.of(maxTransNum);
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[9];
        ByteBuffer.wrap(b).put((byte) getType().getValue()).putLong(maxTransNum);
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        return Optional.of(new BeginCheckpointLogRecord(buf.getLong()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        BeginCheckpointLogRecord that = (BeginCheckpointLogRecord) o;
        return maxTransNum == that.maxTransNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxTransNum);
    }

    @Override
    public String toString() {
        return "BeginCheckpointLogRecord{" +
               "maxTransNum=" + maxTransNum +
               ", LSN=" + LSN +
               '}';
    }
}
