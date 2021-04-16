package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.function.Consumer;

/**
 * An record of the log.
 */
public abstract class LogRecord {
    // LSN of this record, or null if not set - this is not actually
    // stored on disk, and is only set by the log manager for convenience
    protected Long LSN;
    // type of this record
    protected LogType type;

    // method called when redo() is called - only used for testing
    private static Consumer<LogRecord> onRedo = t -> {};

    protected LogRecord(LogType type) {
        this.type = type;
        this.LSN = null;
    }

    /**
     * @return type of log entry as enumerated in LogType
     */
    public final LogType getType() {
        return type;
    }

    /**
     * @return LSN of the log entry
     */
    public final long getLSN() {
        if (LSN == null) {
            throw new IllegalStateException("LSN not set, has this log record been through a log manager call yet?");
        }
        return LSN;
    }

    /**
     * Sets the LSN of a record
     * @param LSN LSN intended to assign to a record
     */
    final void setLSN(Long LSN) {
        this.LSN = LSN;
    }

    /**
     * Gets the transaction number of a log record, if applicable
     * @return optional instance containing transaction number
     */
    public Optional<Long> getTransNum() {
        return Optional.empty();
    }

    /**
     * Gets the LSN of the previous record written by the same transaction
     * @return optional instance containing prevLSN
     */
    public Optional<Long> getPrevLSN() {
        return Optional.empty();
    }

    /**
     * Gets the LSN of record to undo next, if applicable
     * @return optional instance containing transaction number
     */
    public Optional<Long> getUndoNextLSN() {
        return Optional.empty();
    }

    /**
     * @return optional instance containing page number
     * of data that is changed by transaction
     */
    public Optional<Long> getPageNum() {
        return Optional.empty();
    }

    /**
     * @return optional instance containing partition number
     * of data that is changed by transaction
     */
    public Optional<Integer> getPartNum() {
        return Optional.empty();
    }

    public Optional<Long> getMaxTransactionNum() {
        return Optional.empty();
    }

    /**
     * Gets the dirty page table written to the log record, if applicable.
     */
    public Map<Long, Long> getDirtyPageTable() {
        return Collections.emptyMap();
    }

    /**
     * Gets the transaction table written to the log record, if applicable.
     */
    public Map<Long, Pair<Transaction.Status, Long>> getTransactionTable() {
        return Collections.emptyMap();
    }

    /**
     * Gets the table of transaction numbers mapped to page numbers of
     * pages that were touched by the corresponding transaction.
     */
    public Map<Long, List<Long>> getTransactionTouchedPages() {
        return Collections.emptyMap();
    }

    /**
     * @return boolean indicating whether transaction recorded in the
     * log record is undoable
     */
    public boolean isUndoable() {
        return false;
    }

    /**
     * @return boolean indicating whether transaction recorded in the
     * log record is redoable
     */
    public boolean isRedoable() {
        return false;
    }

    /**
     * Returns a CLR undoing this log record, but does not execute the undo.
     * @param lastLSN lastLSN for the CLR
     * @return the CLR corresponding to this log record.
     */
    public LogRecord undo(long lastLSN) {
        throw new UnsupportedOperationException("cannot undo this record: " + this);
    }

    /**
     * Performs the change described by this log record
     * @param rm the database's recovery manager.
     * @param dsm the database's disk space manager
     * @param bm the database's buffer manager
     */
    public void redo(RecoveryManager rm, DiskSpaceManager dsm, BufferManager bm) {
        onRedo.accept(this);
        if (!isRedoable()) {
            throw new UnsupportedOperationException("cannot redo this record: " + this);
        }
    }

    /**
     * Log records are serialized as follows:
     *
     *  - a 1-byte integer indicating the type of log record, followed by
     *  - a variable number of bytes depending on log record (see specific
     *    LogRecord implementations for details).
     */
    public abstract byte[] toBytes();

    /**
     * Load a log record from a buffer.
     * @param buf Buffer containing a serialized log record.
     * @return The log record, or Optional.empty() if logType == 0 (marker for no record)
     * @throws UnsupportedOperationException if log type is not recognized
     */
    public static Optional<LogRecord> fromBytes(Buffer buf) {
        int type;
        try {
            type = buf.get();
        } catch (PageException e) {
            return Optional.empty();
        }
        if (type == 0) {
            return Optional.empty();
        }
        switch (LogType.fromInt(type)) {
        case MASTER:
            return MasterLogRecord.fromBytes(buf);
        case ALLOC_PAGE:
            return AllocPageLogRecord.fromBytes(buf);
        case UPDATE_PAGE:
            return UpdatePageLogRecord.fromBytes(buf);
        case FREE_PAGE:
            return FreePageLogRecord.fromBytes(buf);
        case ALLOC_PART:
            return AllocPartLogRecord.fromBytes(buf);
        case FREE_PART:
            return FreePartLogRecord.fromBytes(buf);
        case COMMIT_TRANSACTION:
            return CommitTransactionLogRecord.fromBytes(buf);
        case ABORT_TRANSACTION:
            return AbortTransactionLogRecord.fromBytes(buf);
        case END_TRANSACTION:
            return EndTransactionLogRecord.fromBytes(buf);
        case BEGIN_CHECKPOINT:
            return BeginCheckpointLogRecord.fromBytes(buf);
        case END_CHECKPOINT:
            return EndCheckpointLogRecord.fromBytes(buf);
        case UNDO_ALLOC_PAGE:
            return UndoAllocPageLogRecord.fromBytes(buf);
        case UNDO_UPDATE_PAGE:
            return UndoUpdatePageLogRecord.fromBytes(buf);
        case UNDO_FREE_PAGE:
            return UndoFreePageLogRecord.fromBytes(buf);
        case UNDO_ALLOC_PART:
            return UndoAllocPartLogRecord.fromBytes(buf);
        case UNDO_FREE_PART:
            return UndoFreePartLogRecord.fromBytes(buf);
        default:
            throw new UnsupportedOperationException("bad log type");
        }
    }

    /**
     * Set the method called whenever redo() is called on a LogRecord. This
     * is only to be used for testing.
     * @param handler method to be called whenever redo() is called
     */
    static void onRedoHandler(Consumer<LogRecord> handler) {
        onRedo = handler;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        LogRecord logRecord = (LogRecord) o;
        return type == logRecord.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public String toString() {
        return "LogRecord{" +
               "type=" + type +
               '}';
    }
}
