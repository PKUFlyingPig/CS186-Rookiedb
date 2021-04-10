package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;

/**
 * Interface for a recovery manager.
 */
public interface RecoveryManager extends AutoCloseable {
    /**
     * Initializes the log; only called the first time the database is set up.
     */
    void initialize();

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager);

    /**
     * Called when a new transaction is started.
     * @param transaction new transaction
     */
    void startTransaction(Transaction transaction);

    /**
     * Called when a transaction is about to start committing.
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    long commit(long transNum);

    /**
     * Called when a transaction is set to be aborted.
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    long abort(long transNum);

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    long end(long transNum);

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    void pageFlushHook(long pageLSN);

    /**
     * Called when a page has been updated on disk.
     * @param pageNum page number of page updated on disk
     */
    void diskIOHook(long pageNum);

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * must be the same length.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
    */
    long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                      byte[] after);

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    long logAllocPart(long transNum, int partNum);

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    long logFreePart(long transNum, int partNum);

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    long logAllocPage(long transNum, long pageNum);

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    long logFreePage(long transNum, long pageNum);

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    void savepoint(long transNum, String name);

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    void releaseSavepoint(long transNum, String name);

    /**
     * Rolls back transaction to a savepoint.
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    void rollbackToSavepoint(long transNum, String name);

    /**
     * Creates a checkpoint.
     */
    void checkpoint();

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    void flushToLSN(long LSN);

    /**
     * Adds the given page number and LSN to the dirty page table if the page
     * is not already present.
     * @param pageNum
     * @param LSN
     */
    void dirtyPage(long pageNum, long LSN);

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * New transactions may be started once this method returns.
     */
    void restart();

    /**
     * Clean up: log flush, checkpointing, etc. Called when the database is closed.
     */
    @Override
    void close();
}
