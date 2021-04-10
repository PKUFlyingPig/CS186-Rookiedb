package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;

import java.util.Objects;
import java.util.Optional;

public class BeginCheckpointLogRecord extends LogRecord {
    public BeginCheckpointLogRecord() {
        super(LogType.BEGIN_CHECKPOINT);
    }

    @Override
    public byte[] toBytes() {
        byte[] b = new byte[1];
        ByteBuffer.wrap(b).put((byte) getType().getValue());
        return b;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        return Optional.of(new BeginCheckpointLogRecord());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        return true; // Begin Checkpoints are indistinguishable from each other
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public String toString() {
        return "BeginCheckpointLogRecord{" +
               ", LSN=" + LSN +
               '}';
    }
}
