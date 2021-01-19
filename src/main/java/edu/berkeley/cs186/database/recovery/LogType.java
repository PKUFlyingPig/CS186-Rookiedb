package edu.berkeley.cs186.database.recovery;

public enum LogType {
    // master log record (stores current checkpoint)
    MASTER,
    // log record for allocating a new page (via the disk space manager)
    ALLOC_PAGE,
    // log record for updating part of a page
    UPDATE_PAGE,
    // log record for freeing a page (via the disk space manager)
    FREE_PAGE,
    // log record for allocating a new partition (via the disk space manager)
    ALLOC_PART,
    // log record for freeing a partition (via the disk space manager)
    FREE_PART,
    // log record for starting a transaction commit
    COMMIT_TRANSACTION,
    // log record for starting a transaction abort
    ABORT_TRANSACTION,
    // log record for after a transaction has completely finished
    END_TRANSACTION,
    // log record for start of a checkpoint
    BEGIN_CHECKPOINT,
    // log record for finishing a checkpoint; there may be multiple of these
    // for a checkpoint
    END_CHECKPOINT,
    // compensation log record for undoing a page alloc
    UNDO_ALLOC_PAGE,
    // compensation log record for undoing a page update
    UNDO_UPDATE_PAGE,
    // compensation log record for undoing a page free
    UNDO_FREE_PAGE,
    // compensation log record for undoing a partition alloc
    UNDO_ALLOC_PART,
    // compensation log record for undoing a partition free
    UNDO_FREE_PART;

    private static LogType[] values = LogType.values();

    public int getValue() {
        return ordinal() + 1;
    }

    public static LogType fromInt(int x) {
        if (x < 1 || x > values.length) {
            String err = String.format("Unknown TypeId ordinal %d.", x);
            throw new IllegalArgumentException(err);
        }
        return values[x - 1];
    }
}
