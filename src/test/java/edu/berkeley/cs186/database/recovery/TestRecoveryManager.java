package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj5Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.DiskSpaceManagerImpl;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;
import edu.berkeley.cs186.database.recovery.records.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

@Category({Proj5Tests.class})
public class TestRecoveryManager {
    private String testDir;
    private ARIESRecoveryManager recoveryManager;
    private LogManager logManager;
    private DiskSpaceManager diskSpaceManager;
    private BufferManager bufferManager;
    private Map<Long, Long> dirtyPageTable;
    private Map<Long, TransactionTableEntry> transactionTable;
    private final Queue<Consumer<LogRecord>> redoMethods = new ArrayDeque<>();

    // 3 seconds per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (3000 * TimeoutScaling.factor)));

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        testDir = tempFolder.newFolder("test-dir").getAbsolutePath();
        recoveryManager = loadRecoveryManager(testDir);
        DummyTransaction.cleanupTransactions();
        LogRecord.onRedoHandler(t -> {
        });
    }

    @After
    public void cleanup() {
        recoveryManager.close();
    }

    /**
     * Loads the recovery manager from disk.
     *
     * @param dir testDir
     * @return recovery manager, loaded from disk
     */
    protected ARIESRecoveryManager loadRecoveryManager(String dir) {
        ARIESRecoveryManager recoveryManager = new ARIESRecoveryManager(DummyTransaction::create);
        DiskSpaceManager diskSpaceManager = new DiskSpaceManagerImpl(dir, recoveryManager);
        BufferManager bufferManager = new BufferManager(diskSpaceManager, recoveryManager, 32,
                new LRUEvictionPolicy());
        boolean isLoaded = true;
        try {
            diskSpaceManager.allocPart(0);
            diskSpaceManager.allocPart(1);
            for (int i = 0; i < 10; ++i) {
                diskSpaceManager.allocPage(DiskSpaceManager.getVirtualPageNum(1, i));
            }
            isLoaded = false;
        } catch (IllegalStateException e) {
            // already loaded
        }
        recoveryManager.setManagers(diskSpaceManager, bufferManager);
        if (!isLoaded) {
            recoveryManager.initialize();
        }
        this.logManager = recoveryManager.logManager;
        this.bufferManager = recoveryManager.bufferManager;
        this.diskSpaceManager = recoveryManager.diskSpaceManager;
        this.transactionTable = recoveryManager.transactionTable;
        this.dirtyPageTable = recoveryManager.dirtyPageTable;
        return recoveryManager;
    }

    /**
     * Tests transaction abort. Transactions T1 and T2 are created, T2 aborts:
     * Checks:
     *  - Transaction table's lastLSN for T1 is the abort record LSN
     *  - T1 and T2's transaction statuses are correct
     */
    @Test
    @Category(PublicTests.class)
    public void testAbort() {
        // Create T1
        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        // Create T2
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        // T1 aborts
        long abortLSN = recoveryManager.abort(1L);

        // lastLSN should be updated to the LSN of the abort log
        assertEquals(abortLSN, transactionTable.get(1L).lastLSN);

        // T1 should be in the aborting state, T2 should still be running
        assertEquals(Transaction.Status.ABORTING, transactionTable.get(1L).transaction.getStatus());
        assertEquals(Transaction.Status.RUNNING, transactionTable.get(2L).transaction.getStatus());
    }

    /**
     * Tests that aborting + ending a transaction correctly undoes its changes:
     * 1. Sets up and executes a log w/ 5 log records (see code for record types),
     *    the transaction table. T2 completes aborting. T1 aborts but does not end
     * 2. After aborting, RecoveryManager.end() is called on T1
     *    Checks:
     *      - AllocPartLogRecord and UpdatePageLogRecord are undone
     *      - Nothing else is undone
     * 3. Checks state after aborting:
     *      - Log contains correct CLR records and EndTransaction record
     *      - DPT contains 10000000001L modified from undoing the page update
     *      - Transaction table (empty) and transaction status have correct values.
     */
    @Test
    @Category(PublicTests.class)
    public void testAbortingEnd() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x00, (byte) 0x00, (byte) 0x0D };

        Transaction t1 = DummyTransaction.create(1L);
        Transaction t2 = DummyTransaction.create(2L);

        // 1. Set up transactions, append and execute logs
        recoveryManager.startTransaction(t1);
        recoveryManager.startTransaction(t2);

        // The following logs are appended
        // Record Type      | transNum | LSN     | prevLSN | Other Details
        // -----------------+----------+---------+---------+---------------
        // UpdatePage       |        1 |   10000 |       0 | pageNum=10000000001
        // AllocPart        |        1 |   20000 |   10000 | partNum=7
        // AbortTransaction |        2 |   30000 |       0 |
        // AbortTransaction |        1 |   40000 |   20000 |
        // EndTransaction   |        2 |   50000 |   30000 |
        logManager.flushToLSN(9999L); // force next record to be LSN 10000L
        LogRecord updateRecord = new UpdatePageLogRecord(t1.getTransNum(), 10000000001L, 0L, (short) 71, before, after);
        logManager.appendToLog(updateRecord);
        logManager.flushToLSN(19999L); // force next record to be LSN 20000L
        LogRecord allocRecord = new AllocPartLogRecord(t1.getTransNum(), 7, 10000L);
        logManager.appendToLog(allocRecord);
        logManager.flushToLSN(29999L); // force next record to be LSN 30000L
        logManager.appendToLog(new AbortTransactionLogRecord(t2.getTransNum(), 0L)); // random log
        logManager.flushToLSN(39999L); // force next record to be LSN 40000L
        logManager.appendToLog(new AbortTransactionLogRecord(t1.getTransNum(), 20000L));
        logManager.flushToLSN(49999L); // force next record to be LSN 50000L
        logManager.appendToLog(new EndTransactionLogRecord(t2.getTransNum(), 30000L)); // random log
        logManager.flushToLSN(59999L); // force next record to be LSN 60000L

        // Execute the update/alloc records so that changes can be undone
        updateRecord.redo(recoveryManager, diskSpaceManager, bufferManager);
        allocRecord.redo(recoveryManager, diskSpaceManager, bufferManager);

        // Empty out the DPT
        recoveryManager.redoComplete = true; // Must be true for DPT to flush
        bufferManager.evictAll();

        // Manually set T1's lastLSN to the Abort record's LSN. Update status.
        recoveryManager.transactionTable.get(t1.getTransNum()).lastLSN = 40000L;
        t1.setStatus(Transaction.Status.ABORTING);

        // Manually remove T2 from transaction table. Update status.
        recoveryManager.transactionTable.remove(t2.getTransNum());
        t2.setStatus(Transaction.Status.COMPLETE);

        // 2. T1 ends. T1's alloc record and update record should be undone
        ///   (redo should be called on their CLRs)
        LogRecord expectedAllocCLR = allocRecord.undo(40000L);
        expectedAllocCLR.setLSN(60000L);
        LogRecord expectedUpdateCLR = updateRecord.undo(60000L);
        expectedUpdateCLR.setLSN(70000L);

        setupRedoChecks(
            record -> assertEquals(expectedAllocCLR, record),
            record -> assertEquals(expectedUpdateCLR, record)
        );
        recoveryManager.end(t1.getTransNum());
        finishRedoChecks();

        // 3. Check state after ending
        Iterator<LogRecord> logs = logManager.scanFrom(60000L);

        // The CLR for the alloc record and the CLR for the update record have
        // been appended.
        assertEquals(expectedAllocCLR, logs.next());
        LogRecord updateCLR = logs.next();
        assertEquals(expectedUpdateCLR, updateCLR);

        // An end transaction log record should have been appended
        LogRecord expectedEnd = new EndTransactionLogRecord(t1.getTransNum(), updateCLR.getLSN());
        expectedEnd.setLSN(expectedUpdateCLR.getLSN() + expectedUpdateCLR.toBytes().length);
        assertEquals(expectedEnd, logs.next());
        assertFalse(logs.hasNext()); // no other records written

        // Transaction should have completed and removed from the transaction table.
        // Redoing the updateCLR should have dirtied page 10000000001.
        assertEquals(Transaction.Status.COMPLETE, t1.getStatus());
        assertTrue(transactionTable.isEmpty());
        assertEquals(Collections.singletonMap(10000000001L, updateCLR.getLSN()), dirtyPageTable);
    }

    /**
     * Basic test of a transaction updating and committing:
     * 1. Transaction 1 logs a page update and commits
     *    Checks:
     *      - LastLSN in transaction table is equal to the LSN of the commit record
     *      - Transaction 1 status is committing
     * 2. Transaction 2 starts and logs a page update. Does not commit
     *    Checks:
     *      - LSN of T1 commit <= flushed LSN < LSN of T2 page write (log is flushed up to the commit record)
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleCommit() {
        // Details for page update
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        // Create Transaction 1
        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        // Transaction 1 performs a write and then commits.
        recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after);
        long commitLSN = recoveryManager.commit(1L);

        // lastLSN should be set to commit record LSN, status should be updated
        assertEquals(commitLSN, transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.COMMITTING, transactionTable.get(1L).transaction.getStatus());

        // Since Transaction1 committed, the log should be flushed up to the
        // commit record's LSN
        assertTrue(logManager.getFlushedLSN() + " is not greater than or equal to " + commitLSN,
                logManager.getFlushedLSN() >= commitLSN);

        // Create Transaction 2
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        // Transaction 2 performs a write
        long updateLSN = recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after);

        // You must implement logPageWrite to pass this check. Checks that log
        // isn't automatically flushed for page updates.
        assertTrue(logManager.getFlushedLSN() + " is not less than " + updateLSN,
                logManager.getFlushedLSN() < updateLSN);
    }

    /**
     * Tests functionality of end:
     * 1. T1 and T2 log a combination of writes and allocs. T2 commits.
     *    Checks:
     *      - LastLSNs in transaction table are updated correctly
     *      - Should be flushed up to the commit record
     *      - T2's status is committing
     * 2. T2 ends and T1 aborts
     *    Checks:
     *      - T1's status is aborting
     *      - T1's lastLSN in the transaction table is updated with the abort record LSN
     * 3. T1 ends and should cause a rollback.
     *    Checks:
     *      - Appropriate number of each record type on the log (including the CLR records from the rollback)
     *          - See code below for the breakdown
     *      - DPT stays the same
     *      - FlushedLSN is still at the commit record
     *      - Transaction statuses are COMPLETE and transaction table is empty
     */
    @Test
    @Category(PublicTests.class)
    public void testEnd() {
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        // 1. T1 and T2 log a combination of writes and allocs. T2 commits.
        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        // Mix of writes and allocs from both T1 and T2. T2 commits.
        long[] LSNs = new long[] {
                recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 0
                recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after), // 1
                recoveryManager.logPageWrite(1L, pageNum, pageOffset, after, before), // 2
                recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, after, before), // 3
                recoveryManager.logAllocPart(2L, 2), // 4
                recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 5
                recoveryManager.commit(2L), // 6
                recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 7
                -1L, -1L,
        };

        // lastLSNs should be set to LSN of transaction's last appended record
        assertEquals(LSNs[7], transactionTable.get(1L).lastLSN);
        assertEquals(LSNs[6], transactionTable.get(2L).lastLSN);

        // Since T2 committed, log should be flushed up to T2's commit record
        // and should have status set ti committing.
        assertEquals(LogManager.maxLSN(LogManager.getLSNPage(LSNs[6])), logManager.getFlushedLSN());
        assertEquals(Transaction.Status.COMMITTING, transactionTable.get(2L).transaction.getStatus());

        // 2. T2 ends and T1 aborts. LSNs and statuses should be updated.
        LSNs[8] = recoveryManager.end(2L);
        LSNs[9] = recoveryManager.abort(1L);

        assertEquals(LSNs[9], transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.ABORTING, transactionTable.get(1L).transaction.getStatus());

        // 3. T1 ends after an abort, so it's changes should be rolled back.
        recoveryManager.end(1L); // 4 CLRs + END should be generated

        // Count number of each type of record in the log. Initialize all
        // counts to 0.
        int totalRecords, abort, commit, end, update, allocPart, undo;
        totalRecords = abort = commit = end = update = allocPart = undo = 0;

        Iterator<LogRecord> logs = logManager.iterator();
        while (logs.hasNext()) {
            LogRecord record = logs.next();
            totalRecords++;
            switch (record.getType()) {
                case ABORT_TRANSACTION: abort++; break;
                case COMMIT_TRANSACTION: commit++; break;
                case END_TRANSACTION: end++;  break;
                case UPDATE_PAGE: update++; break;
                case UNDO_UPDATE_PAGE: undo++; break;
                case ALLOC_PART: allocPart++; break;
                default: break;
            }
        }
        // 3 (master + begin/end checkpoint) + 10 (LSNs) + 4 (CLRs) + 1 (END)
        assertEquals(18, totalRecords);
        assertEquals(1, abort);
        assertEquals(1, commit);
        assertEquals(2, end);
        assertEquals(6, update);
        assertEquals(1, allocPart);
        assertEquals(4, undo);

        // Updates should have dirtied these pages
        assertEquals(LSNs[0], (long) dirtyPageTable.get(pageNum));
        assertEquals(LSNs[1], (long) dirtyPageTable.get(pageNum + 1));

        // Both transactions ended
        assertTrue(transactionTable.isEmpty());
        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertEquals(Transaction.Status.COMPLETE, transaction2.getStatus());

        // Log should be flushed up to T2's commit record
        assertEquals(LogManager.maxLSN(LogManager.getLSNPage(LSNs[6])), logManager.getFlushedLSN());
    }

    /**
     * Tests logging a page update.
     *
     * Does the following:
     * 1. Transaction 1 logs a page update on 10000000002L
     * 2. Transaction 2 logs a page update on 10000000003L
     * 3. Transaction 1 logs another page write on 10000000002L again
     * Checks:
     *  - Transaction table contains each transaction with the correct lastLSN value
     *  - Dirty page table contains page modified by this update with correct recLSN
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleLogPageWrite() {
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long LSN1 = recoveryManager.logPageWrite(transaction1.getTransNum(), 10000000002L, pageOffset, before, after);

        // Check DPT, X-Act table updates
        assertTrue(transactionTable.containsKey(1L));
        assertEquals(LSN1, transactionTable.get(1L).lastLSN);
        assertTrue(dirtyPageTable.containsKey(10000000002L));
        assertEquals(LSN1, (long) dirtyPageTable.get(10000000002L));

        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        long LSN2 = recoveryManager.logPageWrite(transaction2.getTransNum(), 10000000003L, pageOffset, before, after);

        assertTrue(transactionTable.containsKey(2L));
        assertEquals(LSN2, transactionTable.get(2L).lastLSN);
        assertEquals(LSN2, (long) dirtyPageTable.get(10000000003L));

        long LSN3 = recoveryManager.logPageWrite(transaction1.getTransNum(), 10000000002L, pageOffset, before, after);
        assertEquals(LSN3, transactionTable.get(1L).lastLSN);
        assertEquals(LSN1, (long) dirtyPageTable.get(10000000002L));
    }

    /**
     * Tests rolling back to a savepoint:
     * 1. T1 sets a savepoint, logs a page write, then rolls back to the savepoint
     *    Checks:
     *      - Log contains two records: the page update record, and the CLR record for that update
     *      - CLR record contains the correct values (offset, after, transaction number, page number, undoNextLSN)
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleSavepoint() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        recoveryManager.savepoint(1L, "savepoint 1");
        long LSN = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        recoveryManager.rollbackToSavepoint(1L, "savepoint 1");

        Iterator<LogRecord> logs = logManager.scanFrom(LSN);
        logs.next(); // page write record

        LogRecord clr = logs.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, clr.getType());

        assertEquals((short) 0, ((UndoUpdatePageLogRecord) clr).offset);
        assertArrayEquals(before, ((UndoUpdatePageLogRecord) clr).after);
        assertEquals(Optional.of(1L), clr.getTransNum());
        assertEquals(Optional.of(10000000001L), clr.getPageNum());
        assertTrue(clr.getUndoNextLSN().orElseThrow(NoSuchElementException::new) < LSN);
    }

    /**
     * Tests basic checkpoint:
     *  - T1 logs a write, checkpoints, logs 2 additional writes
     *    Checks:
     *      - Logs one begin checkpoint record and one end checkpoint record
     *      - Check that the dpt and transaction table in the checkpoint record contain the correct contents
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleCheckpoint() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long firstWriteLSN = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        // The state of the DPT should be stored in an end checkpoint log record
        recoveryManager.checkpoint();

        // DPT in checkpoint should not be affected by these writes
        recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, after, before);
        recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        Iterator<LogRecord> logs = logManager.scanFrom(firstWriteLSN);
        logs.next(); // page write (firstWriteLSN)

        LogRecord beginCheckpoint = logs.next();
        LogRecord endCheckpoint = logs.next();
        assertEquals(LogType.BEGIN_CHECKPOINT, beginCheckpoint.getType());
        assertEquals(LogType.END_CHECKPOINT, endCheckpoint.getType());

        Map<Long, Pair<Transaction.Status, Long>> txnTable = endCheckpoint.getTransactionTable();
        Map<Long, Long> dpt = endCheckpoint.getDirtyPageTable();
        assertEquals(firstWriteLSN, (long) dpt.get(10000000001L));
        assertEquals(new Pair<>(Transaction.Status.RUNNING, firstWriteLSN), txnTable.get(1L));
    }

    /**
     * Tests that end checkpoints are appended when as full as possible:
     *  - DPT is filled with 200 entries, and the transaction table is filled
     *    with 200 entries. Afterwards, a checkpoint is created.
     *    Checks:
     *      - First end checkpoint contains all of the DPT entries and as many
     *        possible transaction table entries that can fit in the remaining
     *        space (in this case, 52)
     *      - Second end checkpoint contains the remaining transaction table
     *        entries (in this case, 200 - 52 = 148)
     */
    @Test
    @Category(PublicTests.class)
    public void testFullCheckpoint() {
        // Create 200 DPT entries and 200 transaction table entries
        for (long l = 1; l <= 200; l++) {
            dirtyPageTable.put(l, l*l);

            Transaction t = DummyTransaction.create(l);
            recoveryManager.startTransaction(t);
            // Sets status to one of RUNNING, COMMITTING or ABORTING
            t.setStatus(Transaction.Status.fromInt((int) l % 3));
            TransactionTableEntry entry = new TransactionTableEntry(t);
            entry.lastLSN = l*l;
            transactionTable.put(l, entry);
        }

        // Perform checkpoint
        recoveryManager.checkpoint();

        Iterator<LogRecord> logs = logManager.scanFrom(10000L);

        // Next 3 logs should be from the checkpoint
        LogRecord beginCheckpoint = logs.next();
        LogRecord endCheckpoint1 = logs.next();
        LogRecord endCheckpoint2 = logs.next();
        assertEquals(LogType.BEGIN_CHECKPOINT, beginCheckpoint.getType());
        assertEquals(LogType.END_CHECKPOINT, endCheckpoint1.getType());
        assertEquals(LogType.END_CHECKPOINT, endCheckpoint2.getType());
        assertFalse(logs.hasNext());

        // Sanity check: If we have 200 DPT entries and 52 transaction table
        // entries, there is no extra space for more transaction table entries.
        assertTrue(EndCheckpointLogRecord.fitsInOneRecord(200, 52));
        assertFalse(EndCheckpointLogRecord.fitsInOneRecord(200, 53));

        // First end checkpoint should have all the DPT entries, and 52
        // transaction table entries
        assertEquals(200, endCheckpoint1.getDirtyPageTable().size());
        assertEquals(52, endCheckpoint1.getTransactionTable().size());

        // Second end checkpoint should have no DPT entries, and the remaining
        // transaction table entries.
        assertEquals(0, endCheckpoint2.getDirtyPageTable().size());
        assertEquals(148, endCheckpoint2.getTransactionTable().size());

        // Check the contents of the checkpoint DPT/transaction tables match
        // what we inserted earlier
        for (long l = 1; l <= 200; l++) {
            assertEquals(l*l, (long) endCheckpoint1.getDirtyPageTable().get(l));
            Pair<Transaction.Status, Long> p;
            if (endCheckpoint1.getTransactionTable().containsKey(l)) {
                p = endCheckpoint1.getTransactionTable().get(l);
            } else if (endCheckpoint2.getTransactionTable().containsKey(l)) {
                p = endCheckpoint2.getTransactionTable().get(l);
            } else {
                fail("Transaction #" + l + " could not be found in checkpoint ttables");
                return;
            }
            // Status
            assertEquals(p.getFirst(), Transaction.Status.fromInt((int) l % 3));
            // prevLSN
            assertEquals(l*l, (long) p.getSecond());
        }
    }

    /**
     * Test rolling back T2 while T1 is also running:
     * 1. T1 writes, T2 writes, T2 makes savepoint, T1 and T2 continue writing
     * 2. T2 rolls back to savepoint
     *    Checks:
     *      - Nothing undone (all records for T2 after the savepoint have already been undone)
     *      - Checks that DPT and transaction table remain intact
     *      - T1 still running
     */
    @Test
    @Category(PublicTests.class)
    public void testNestedRollback() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x00, (byte) 0x00, (byte) 0x0D };

        Transaction t1 = DummyTransaction.create(1L);
        Transaction t2 = DummyTransaction.create(2L);

        // 1. T1 writes, T2 writes, T2 makes savepoint, T1 and T2 continue writing
        recoveryManager.startTransaction(t1);
        recoveryManager.startTransaction(t2);

        logManager.flushToLSN(9999L); // force next record to be LSN 10000L
        LogRecord updateRecord1 = new UpdatePageLogRecord(t1.getTransNum(), 10000000001L, 0L, (short) 71,
                before, after);
        logManager.appendToLog(updateRecord1);
        updateRecord1.redo(recoveryManager, diskSpaceManager, bufferManager);

        logManager.flushToLSN(19999L); // force next record to be LSN 20000L
        LogRecord updateRecord2 = new UpdatePageLogRecord(t2.getTransNum(), 10000000001L, 0L, (short) 33,
                before, after);
        logManager.appendToLog(updateRecord2);
        updateRecord2.redo(recoveryManager, diskSpaceManager, bufferManager);

        transactionTable.get(t2.getTransNum()).lastLSN = updateRecord2.getLSN();
        recoveryManager.savepoint(t2.getTransNum(), "savepoint");

        logManager.flushToLSN(29999L); // force next record to be LSN 30000L
        LogRecord updateRecord3 = new UpdatePageLogRecord(t2.getTransNum(), 10000000001L, 20000L,
                (short) 11,
                before, after);
        logManager.appendToLog(updateRecord3);
        updateRecord3.redo(recoveryManager, diskSpaceManager, bufferManager);

        logManager.flushToLSN(39999L); // force next record to be LSN 40000L
        LogRecord updateRecord4 = new UpdatePageLogRecord(t1.getTransNum(), 10000000002L, 10000L,
                (short) 991,
                before, after);
        logManager.appendToLog(updateRecord4);
        updateRecord4.redo(recoveryManager, diskSpaceManager, bufferManager);

        logManager.flushToLSN(49999L); // force next record to be LSN 50000L
        LogRecord updateRecord5 = new UndoUpdatePageLogRecord(t2.getTransNum(), 10000000001L, 30000L,
                20000L,
                (short) 11, before);
        logManager.appendToLog(updateRecord5);
        updateRecord5.redo(recoveryManager, diskSpaceManager, bufferManager);

        logManager.flushToLSN(59999L); // force next record to be LSN 60000L

        // Flush buffer manager + DPT
        recoveryManager.redoComplete = true; // Must be set to true to flush DPT
        bufferManager.evictAll();

        // Manually set DPT and transaction table values
        dirtyPageTable.put(10000000001L, 10000L);
        dirtyPageTable.put(10000000003L, 40000L);
        transactionTable.get(t1.getTransNum()).lastLSN = 40000L;
        transactionTable.get(t2.getTransNum()).lastLSN = 50000L;
        Map<Long, Long> expectedDPT = new HashMap<>(dirtyPageTable);
        Map<Long, TransactionTableEntry> expectedTxnTable = new HashMap<>(transactionTable);

        long initNumIOs = bufferManager.getNumIOs();
        setupRedoChecks(); // Intentionally empty, no CLRs should be created
        recoveryManager.rollbackToSavepoint(t2.getTransNum(), "savepoint");
        finishRedoChecks();
        long finalNumIOs = bufferManager.getNumIOs();

        // read CLR (1 I/O)
        assertEquals(1L, finalNumIOs - initNumIOs);

        // 2. T2 rolls back to savepoint.
        Iterator<LogRecord> logs = logManager.scanFrom(60000L);
        assertFalse(logs.hasNext()); // no other records written
        assertEquals(59999L, logManager.getFlushedLSN()); // not flushed
        assertEquals(t1.getStatus(), Transaction.Status.RUNNING);
        assertEquals(expectedDPT, dirtyPageTable);
        assertEquals(expectedTxnTable, transactionTable);
    }

    /**
     * Test analysis phase of recovery
     *
     * Does the following:
     * 1. Sets up log (see comments below for table)
     * 2. Simulates database shutdown
     * 3. Runs analysis phase of recovery
     * 4. Checks for correct transaction table and DPT (see comments below for tables)
     * 5. Checks transaction statuses/cleanup statuses
     */
    @Test
    @Category(PublicTests.class)
    public void testRestartAnalysis() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        DummyTransaction transaction1 = DummyTransaction.create(1L);
        DummyTransaction transaction2 = DummyTransaction.create(2L);
        DummyTransaction transaction3 = DummyTransaction.create(3L);

        // 1. Sets up log
        // The code below sets up the following log:
        //       LSN |    transaction |   prevLSN |     pageNum  |             Type
        // ----------+----------------+-----------+--------------+------------------
        //     10000 |              1 |         0 | 10000000001L |           Update
        //     10039 |              1 |        10 | 10000000002L |           Update
        //     10078 |              3 |         0 | 10000000003L |           Update
        //     10117 |              1 |        20 |              |           Commit
        //     10134 |              1 |        40 |              |              End
        //     10151 |              2 |         0 | 10000000001L |        Free Page
        //     10176 |              2 |        60 |              |            Abort
        //     10193 |                |           |              | Begin Checkpoint
        //
        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 0, before, after))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(3L, 10000000003L, 0L, (short) 0, before, after))); // 2
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, LSNs.get(1)))); // 3
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(new FreePageLogRecord(2L, 10000000001L, 0L))); // 5
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(2L, LSNs.get(5)))); // 6
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord())); // 7

        // 2. Simulate database shutdown
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // 3. Run analysis phase of recovery
        recoveryManager.restartAnalysis();

        // 4 Checks for correct transaction table and
        // transaction table after running analysis
        // transaction | lastLSN | touchedPages
        // ------------+---------+--------------
        //           2 |   10176 | 10000000001L
        //           3 |       * | 10000000003L
        // * indicates the LSN of the abort record that was added for transaction 3 at the end of analysis
        assertFalse(transactionTable.containsKey(1L));
        assertTrue(transactionTable.containsKey(2L));
        assertEquals((long) LSNs.get(6), transactionTable.get(2L).lastLSN);
        assertTrue(transactionTable.containsKey(3L));
        assertTrue(transactionTable.get(3L).lastLSN > LSNs.get(7));

        // DPT after running analysis
        //     Page Num  |  recLSN
        // --------------+---------
        //  10000000002L |     20
        //  10000000003L |     30
        //
        assertFalse(dirtyPageTable.containsKey(10000000001L));
        assertTrue(dirtyPageTable.containsKey(10000000002L));
        assertEquals((long) LSNs.get(1), (long) dirtyPageTable.get(10000000002L));
        assertTrue(dirtyPageTable.containsKey(10000000003L));
        assertEquals((long) LSNs.get(2), (long) dirtyPageTable.get(10000000003L));

        // 5. Check transaction status's
        // status/cleanup
        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertTrue(transaction1.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction2.getStatus());
        assertFalse(transaction2.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction3.getStatus());
        assertFalse(transaction2.cleanedUp);

        // FlushedLSN
        assertEquals(LogManager.maxLSN(LogManager.getLSNPage(LSNs.get(7))), logManager.getFlushedLSN());
    }

    /**
     * Tests that transaction table and DPT are updated when encountering
     * checkpoints with new information:
     * 1. The log is set up as follows:
     *     - 2 UpdatePage records for T1. These are before the begin checkpoint,
     *       so they shouldn't be seen during analysis
     *     - 4 UpdatePage records for T2
     *     - EndTransaction for T1
     *     - CommitTransaction For T2
     *     - AbortTransaction for T3
     *     - EndCheckpoint
     * 2. The master record is rewritten and analysis phase is run. Afterwards:
     *     - Analysis should have appended an EndTransaction record for T2,
     *       which was in the committing state
     *     - Analysis should have appended an AbortTransaction record for T4.
     *       T4 should have been found and in the running state while processing
     *       the end checkpoint record.
     * 3. DPT and transaction table are checked.
     */
    @Test
    @Category(PublicTests.class)
    public void testAnalysisCheckpoint() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 0, before, after))); // 1
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord())); // 2
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(2L, 10000000001L, 0L, (short) 0, before, after))); // 3
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(2L, 10000000002L, LSNs.get(3), (short) 0, before, after))); // 4
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(2L, 10000000003L, LSNs.get(4), (short) 0, after, before))); // 5
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(2L, 10000000004L, LSNs.get(5), (short) 0, after, before))); // 6
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(1)))); // 7
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(2L, LSNs.get(6)))); // 8
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(3L, 0L))); // 9
        LSNs.add(logManager.appendToLog(new EndCheckpointLogRecord(
            new HashMap<Long, Long>() {{
                // End checkpoint DPT
                put(10000000001L, LSNs.get(0));
                put(10000000002L, LSNs.get(1));
                put(10000000003L, LSNs.get(5));
                put(10000000004L, LSNs.get(6));
            }},
            new HashMap<Long, Pair<Transaction.Status, Long>>() {{
                // End checkpoint Transaction Table
                put(2L, new Pair<>(Transaction.Status.COMMITTING, LSNs.get(8)));
                put(3L, new Pair<>(Transaction.Status.ABORTING, LSNs.get(9)));
                put(4L, new Pair<>(Transaction.Status.RUNNING, 0L));
            }}
        ))); // 10
        // end/abort records from analysis=
        logManager.rewriteMasterRecord(new MasterLogRecord(LSNs.get(2)));

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        recoveryManager.restartAnalysis();

        // check log
        Iterator<LogRecord> logs = logManager.scanFrom(20000L);
        assertEquals(new EndTransactionLogRecord(2L, LSNs.get(8)), logs.next());
        LogRecord abortRecord = logs.next();
        assertEquals(new AbortTransactionLogRecord(4L, 0), abortRecord);
        assertFalse(logs.hasNext());
        assertEquals(19999L, logManager.getFlushedLSN());

        // T1 and T2 should have ended, and been removed
        assertFalse(transactionTable.containsKey(1l));
        assertFalse(transactionTable.containsKey(2l));

        // Check entries for T3 and T4
        assertEquals(2, transactionTable.size());
        assertTrue(transactionTable.containsKey(3l));
        assertEquals((long) LSNs.get(9), transactionTable.get(3l).lastLSN);
        assertTrue(transactionTable.containsKey(4l));
        assertEquals((long) abortRecord.LSN , transactionTable.get(4l).lastLSN);

        // Check DPT. Should have same entries as in the checkpoint
        assertEquals(LSNs.get(0), dirtyPageTable.get(10000000001L));
        assertEquals(LSNs.get(1), dirtyPageTable.get(10000000002L));
        assertEquals(LSNs.get(5), dirtyPageTable.get(10000000003L));
        assertEquals(LSNs.get(6), dirtyPageTable.get(10000000004L));
        assertEquals(4, dirtyPageTable.size());
    }

    /**
     * Tests that analysis handles checkpoint status information correctly
     * - T1 has ended, but the log is out-of-date (says COMMITTING)
     * - T2 isn't seen until the checkpoint, checkpoint says COMMITTING
     * - T3 has ended, but the log is out-of-date (says ABORTING)
     * - T4 isn't seen until the checkpoint, checkpoint says ABORTING
     * - T5 has committed, but the log is out-of-date (says RUNNING)
     * - T6 has aborted, but the log is out-of-date (says RUNNING)
     */
    @Test
    @Category(PublicTests.class)
    public void testAnalysisFuzzyCheckpointStatus() {
        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, 0L))); // 0
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(2L, 0L))); // 1
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(3L, 0L))); // 2
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(4L, 0L))); // 3
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord())); // 4
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(0)))); // 5
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(3L, LSNs.get(2)))); // 6
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(5L, 0L))); // 7
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(6L, 0L))); // 8
        LSNs.add(logManager.appendToLog(new EndCheckpointLogRecord(
            new HashMap<>(), // empty DPT
            new HashMap<Long, Pair<Transaction.Status, Long>>() {{
                put(1L, new Pair<>(Transaction.Status.COMMITTING, LSNs.get(0)));
                put(2L, new Pair<>(Transaction.Status.COMMITTING, LSNs.get(1)));
                put(3L, new Pair<>(Transaction.Status.ABORTING, LSNs.get(2)));
                put(4L, new Pair<>(Transaction.Status.ABORTING, LSNs.get(3)));
                put(5L, new Pair<>(Transaction.Status.RUNNING, 0L));
                put(6L, new Pair<>(Transaction.Status.RUNNING, 0L));
            }}
        ))); // 9

        logManager.rewriteMasterRecord(new MasterLogRecord(LSNs.get(5)));

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);
        recoveryManager.restartAnalysis();

        // check transaction table
        // T1 should have been completed and removed (log #5)
        System.out.println(transactionTable);
        assertFalse(transactionTable.containsKey(1L));
        // T2 should have been completed and removed (log #9 sets to committing)
        assertFalse(transactionTable.containsKey(2L));
        // T3 should have been completed and removed (log #6)
        assertFalse(transactionTable.containsKey(3L));
        // T4 should have been set to the recovery aborting state (log #9)
        assertTrue(transactionTable.containsKey(4L));
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transactionTable.get(4L).transaction.getStatus());
        // T5 should have been completed and remove (log #7 sets to committing)
        assertFalse(transactionTable.containsKey(5L));
        // T6 should have been set to the recovery aborting state (log #8)
        assertTrue(transactionTable.containsKey(6L));
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transactionTable.get(6L).transaction.getStatus());

        // check log
        Iterator<LogRecord> logs = logManager.scanFrom(20000L);
        assertEquals(new EndTransactionLogRecord(2L, LSNs.get(1)), logs.next());
        assertEquals(new EndTransactionLogRecord(5L, LSNs.get(7)), logs.next());
        assertFalse(logs.hasNext());
    }

    /**
     * Test redo phase of recovery
     *
     * Does the following:
     * 1. Sets up log. Transaction 1 makes a few alloc/updates, commits, and ends.
     * 2. Simulate database shutdown and sets up dpt (to simulate analysis)
     * 3. Runs redo phase and checks that we only redo records 2 - 4 and no
     *    others (since 0 - 1 were already flushed to disk according to dpt)
     */
    @Test
    @Category(PublicTests.class)
    public void testRestartRedo() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };
        DiskSpaceManager dsm = diskSpaceManager;
        BufferManager bm = bufferManager;
        DummyTransaction.create(1L);

        // 1. Set up log. Transaction 1 makes a few alloc/updates, commits, and ends.
        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1, before, after))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(1), (short) 1, after, before))); // 2
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(2), (short) 2, before, after))); // 3
        LSNs.add(logManager.appendToLog(new AllocPartLogRecord(1L, 10, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, LSNs.get(4)))); // 5
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(5)))); // 6

        // actually do the first and second write (and get it flushed to disk)
        logManager.fetchLogRecord(LSNs.get(0)).redo(recoveryManager, dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(recoveryManager, dsm, bm);

        // 2. Simulate database shutdown and set up dpt (to simulate analysis)
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up dirty page table - transaction table is empty (transaction ended)
        dirtyPageTable.put(10000000002L, LSNs.get(2));
        dirtyPageTable.put(10000000003L, LSNs.get(3));

        // 3. Run redo phase
        // set up checks for redo - these get called in sequence with each
        // LogRecord#redo call. Note that the first two updates should not be
        // redone based on the state of the DPT.
        setupRedoChecks(
            record -> assertEquals(LSNs.get(2), record.LSN),
            record -> assertEquals(LSNs.get(3), record.LSN),
            record -> assertEquals(LSNs.get(4), record.LSN)
        );
        recoveryManager.restartRedo();
        finishRedoChecks();
    }

    /**
     * Test undo phase of recovery:
     * 1. Sets up log - T1 makes 4 updates and then aborts.
     * 2. We execute the changes specified in log records, simulate a db
     *    shutdown, and set up transaction table to simulate analysis phase
     * 3. Runs the undo phase
     *    Checks:
     *      - The 4 update records are undone and appended to log
     *      - Transaction table is updated accordingly as we undo
     *      - No other records are undone
     *      - T1 status set to COMPLETE
     */
    @Test
    @Category(PublicTests.class)
    public void testRestartUndo() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };
        DummyTransaction transaction1 = DummyTransaction.create(1L);

        // 1. Set up log - T1 makes 4 updates and then aborts.
        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1, before, after))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(1), (short) 2, before, after))); // 2
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000004L, LSNs.get(2), (short) 3, before, after))); // 3
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(3)))); // 4

        // 2. Execute logs, simulate shutdown, set up transaction table
        // actually do the writes
        for (int i = 0; i < 4; ++i) {
            logManager.fetchLogRecord(LSNs.get(i)).redo(
                    recoveryManager, diskSpaceManager, bufferManager);
        }

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up transaction table - leaving DPT empty
        TransactionTableEntry entry1 = new TransactionTableEntry(transaction1);
        entry1.lastLSN = LSNs.get(4);
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // 3
        // set up checks for undo - these get called in sequence with each
        // LogRecord#redo call (which should be called on CLRs).
        // We expect the CLRs to be applied in the reverse of the original
        // order the updates were applied.
        setupRedoChecks((LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000004L), record.getPageNum());
        }, (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000003L), record.getPageNum());
        }, (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000002L), record.getPageNum());
        }, (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000001L), record.getPageNum());
        });

        recoveryManager.restartUndo();
        finishRedoChecks();
        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
    }

    /**
     * Tests that CLR records are not undone in undo phase of recovery
     *
     * This test does the following:
     * 1. Sets up and executes a log with 6 records (see code for record types). T1 aborts and begins undoing changes.
     * 2. Simulates a database shutdown
     * 3. Performs the undo phase
     *    Checks:
     *      - Only the first log record should be undone, all other log records were already undone (see setupRedoChecks)
     * 4. Checks:
     *      - Transaction status is COMPLETE & transaction table empty
     *      - Log contains CLR record and an EndTransaction record
     */
    @Test
    @Category(PublicTests.class)
    public void testUndoCLR() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        DiskSpaceManager dsm = diskSpaceManager;
        BufferManager bm = bufferManager;

        // 1
        DummyTransaction t1 = DummyTransaction.create(1L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 0, before, after))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(1), (short) 0, before, after))); // 2
        LSNs.add(logManager.appendToLog(logManager.fetchLogRecord(LSNs.get(2)).undo(LSNs.get(2)))); // 3
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(logManager.fetchLogRecord(LSNs.get(1)).undo(LSNs.get(4)))); // 5

        logManager.fetchLogRecord(LSNs.get(0)).redo(recoveryManager, dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(recoveryManager, dsm, bm);
        logManager.fetchLogRecord(LSNs.get(2)).redo(recoveryManager, dsm, bm);
        logManager.fetchLogRecord(LSNs.get(3)).redo(recoveryManager, dsm, bm);
        logManager.fetchLogRecord(LSNs.get(5)).redo(recoveryManager, dsm, bm);

        // 2
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // 3
        // set up transaction table - leaving DPT empty
        TransactionTableEntry entry1 = new TransactionTableEntry(t1);
        entry1.lastLSN = LSNs.get(5);
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // set up checks for undo - these get called in sequence with each
        // LogRecord#redo call
        // (which should be called on CLRs)
        setupRedoChecks((LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000001L), record.getPageNum());
        });

        recoveryManager.restartUndo();

        finishRedoChecks();

        // 4
        assertEquals(Transaction.Status.COMPLETE, t1.getStatus());
        assertFalse(transactionTable.containsKey(1L));

        Iterator<LogRecord> logs = logManager.scanFrom(20000L);

        LogRecord next = logs.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(0)).undo(LSNs.get(5)), next);
        long lastLSN = next.LSN;

        assertEquals(new EndTransactionLogRecord(1L, lastLSN), logs.next());

        assertFalse(logs.hasNext());
    }

    /**
     * Tests that restartUndo properly undoes log records while flushing the log
     * and updates the DPT accordingly:
     * 1. Sets up and executes a log with 4 records (see code for record types)
     * 2. Simulates a database shutdown
     * 3. Performs the undo phase
     *    Checks:
     *      - Correct 3 undo records are undone (assert statements in setupRedoChecks)
     *      - UNDO_ALLOC_PAGE causes a flush
     * 4. Checks:
     *      - Nothing else is undone
     *      - Transaction table empty and transaction status is COMPLETE
     * 5. Checks:
     *      - CLR log records are on the log
     *      - DPT should contain the pages modified by the UNDOs
     */
    @Test
    @Category(PublicTests.class)
    public void testUndoDPTAndFlush() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        DiskSpaceManager dsm = diskSpaceManager;
        BufferManager bm = bufferManager;

        DummyTransaction t1 = DummyTransaction.create(1L);

        // 1. Set up and executes a log with 4 records (update, alloc, update, abort)
        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager.appendToLog(new AllocPageLogRecord(1L, 10000000099L, LSNs.get(0)))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(1), (short) 0, before, after))); // 2
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(2)))); // 3

        logManager.fetchLogRecord(LSNs.get(0)).redo(recoveryManager, dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(recoveryManager, dsm, bm);
        logManager.fetchLogRecord(LSNs.get(2)).redo(recoveryManager, dsm, bm);

        // 2. Simulates a database shutdown
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up transaction table - leaving DPT empty
        TransactionTableEntry entry1 = new TransactionTableEntry(t1);
        entry1.lastLSN = LSNs.get(3);
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // 3. Performs the undo phase
        // set up checks for undo - these get called in sequence with each
        // LogRecord#redo call (which should be called on CLRs)
        LogManager logManager1 = logManager;
        setupRedoChecks((LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000002L), record.getPageNum());
        }, (LogRecord record) -> {
            assertEquals(LogType.UNDO_ALLOC_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals(29999L, logManager1.getFlushedLSN()); // flushed
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000099L), record.getPageNum());
        }, (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000001L), record.getPageNum());
        });

        recoveryManager.restartUndo();

        // 4
        finishRedoChecks();

        assertEquals(Transaction.Status.COMPLETE, t1.getStatus());
        assertFalse(transactionTable.containsKey(1L));

        // 5
        Iterator<LogRecord> logs = logManager.scanFrom(20000L);

        LogRecord next = logs.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(2)).undo(LSNs.get(3)), next);
        long LSN1 = next.LSN;

        next = logs.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(1)).undo(LSN1), next);
        long LSN2 = next.LSN;

        next = logs.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(0)).undo(LSN2), next);
        long LSN3 = next.LSN;

        assertEquals(new EndTransactionLogRecord(1L, LSN3), logs.next());

        assertFalse(logs.hasNext());

        assertEquals(new HashMap<Long, Long>() {
            {
                put(10000000001L, LSN3);
                put(10000000002L, LSN1);
            }
        }, dirtyPageTable);
    }

    /**
     * Tests simple case of recovery in its entirety (analysis, redo, undo):
     * 1. T1 logs one page write, simulate db shutdown
     * 2. Run analysis + redo
     *    Checks:
     *      - Page write is redone
     *      - T1 status is RECOVERY_ABORTING
     *      - Transaction table contains T1 and correct lastLSN
     *      - DPT contains 10000000001L with correct recLSN
     * 3. Runs undo phase
     *    Checks the entire log from the first page write which contains:
     *      - The page write log record
     *      - An abort record issued at the end of analysis
     *      - CLR record for the page write
     *      - An end transaction record issued at the end of undo
     *      - Checkpoint records at the end of recovery
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleRestart() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);
        // 1. T1 logs one page write, simulate db shutdown
        recoveryManager.startTransaction(transaction1);
        long LSN = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // 2. Run analysis + redo
        setupRedoChecks((LogRecord record) -> {
            assertEquals(LSN, record.getLSN());
            assertEquals(LogType.UPDATE_PAGE, record.getType());
        });

        recoveryManager.restartAnalysis();
        recoveryManager.restartRedo();
        finishRedoChecks();

        assertTrue(transactionTable.containsKey(transaction1.getTransNum()));
        TransactionTableEntry entry = transactionTable.get(transaction1.getTransNum());
        assertEquals(Transaction.Status.RECOVERY_ABORTING, entry.transaction.getStatus());
        assertEquals(LSN, (long) dirtyPageTable.get(10000000001L));
        recoveryManager.cleanDPT(); // clean DPT before going into undo phase

        // 3. Run undo phase, followed by checkpoint
        recoveryManager.restartUndo();
        recoveryManager.checkpoint();

        Iterator<LogRecord> logs = logManager.scanFrom(LSN);

        assertEquals(LogType.UPDATE_PAGE, logs.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, logs.next().getType());
        assertEquals(LogType.UNDO_UPDATE_PAGE, logs.next().getType());
        assertEquals(LogType.END_TRANSACTION, logs.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, logs.next().getType());
        assertEquals(LogType.END_CHECKPOINT, logs.next().getType());
        assertFalse(logs.hasNext());
    }

    /**
     * Tests restart in its entirety
     *
     * Does the following:
     * 1. Sets up a log
     *      - T1 writes
     *      - T2 writes
     *      - T1 commits
     *      - T3 writes
     *      - T2 writes
     *      - T1 ends
     *      - T3 writes
     *      - T2 aborts
     * 2. Simulates db shutdown
     * 3. Runs all three phases of recovery
     *    Checks:
     *      - The entire log contains the correct records (see code/comments below for ordering)
     *      - Checkpoint records are written to the log at the end of recovery
     */
    @Test
    @Category(PublicTests.class)
    public void testRestart() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xDE, (byte) 0xCA, (byte) 0xFB, (byte) 0xAD };

        // 1. Set up a log
        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);
        Transaction transaction3 = DummyTransaction.create(3L);
        recoveryManager.startTransaction(transaction3);

        long[] LSNs = new long[] { recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after), // 0
                recoveryManager.logPageWrite(2L, 10000000003L, (short) 0, before, after), // 1
                recoveryManager.commit(1L), // 2
                recoveryManager.logPageWrite(3L, 10000000004L, (short) 0, before, after), // 3
                recoveryManager.logPageWrite(2L, 10000000001L, (short) 0, after, before), // 4
                recoveryManager.end(1L), // 5
                recoveryManager.logPageWrite(3L, 10000000002L, (short) 0, before, after), // 6
                recoveryManager.abort(2), // 7
        };

        // 2. Simulate db shutdown
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // 3. Run all three phases of recovery
        recoveryManager.restart(); // run everything in restart recovery

        Iterator<LogRecord> logs = logManager.iterator();
        assertEquals(LogType.MASTER, logs.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, logs.next().getType());
        assertEquals(LogType.END_CHECKPOINT, logs.next().getType());
        assertEquals(LogType.UPDATE_PAGE, logs.next().getType());
        assertEquals(LogType.UPDATE_PAGE, logs.next().getType());
        assertEquals(LogType.COMMIT_TRANSACTION, logs.next().getType());
        assertEquals(LogType.UPDATE_PAGE, logs.next().getType());
        assertEquals(LogType.UPDATE_PAGE, logs.next().getType());
        assertEquals(LogType.END_TRANSACTION, logs.next().getType());
        assertEquals(LogType.UPDATE_PAGE, logs.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, logs.next().getType());

        LogRecord record = logs.next();
        assertEquals(LogType.ABORT_TRANSACTION, record.getType()); // T3
        long LSN8 = record.LSN;

        record = logs.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 6 (T3's write)
        assertEquals(LSN8, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[3], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN9 = record.LSN;

        record = logs.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 4 (T2's write)
        assertEquals(LSNs[7], (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[1], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN10 = record.LSN;

        record = logs.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 3 (T3's write)
        assertEquals(LSN9, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LogType.END_TRANSACTION, logs.next().getType()); // End record for T3

        record = logs.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 1 (T2's write)
        assertEquals(LSN10, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));

        assertEquals(LogType.END_TRANSACTION, logs.next().getType()); // End record for T2
        assertEquals(LogType.BEGIN_CHECKPOINT, logs.next().getType());
        assertEquals(LogType.END_CHECKPOINT, logs.next().getType());
        assertFalse(logs.hasNext());
    }

    /**
     * Tests that DPT is cleaned up correctly in restart as indicated in the spec.
     * Should only contain pages that are actually dirty.
     */
    @Test
    @Category(PublicTests.class)
    public void testRestartCleanup() {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);

        recoveryManager.startTransaction(transaction1);
        long LSN1 = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);
        long LSN2 = recoveryManager.logPageWrite(1L, 10000000002L, (short) 0, before, after);

        logManager.fetchLogRecord(LSN1).redo(recoveryManager, diskSpaceManager,
                bufferManager);

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);
        recoveryManager.restartAnalysis();
        recoveryManager.restartRedo();
        recoveryManager.cleanDPT();
        assertEquals(Collections.singletonMap(10000000002L, LSN2), dirtyPageTable);
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * Helper to set up checks for redo. The first call to LogRecord.redo will call
     * the first method in METHODS, the second call to the second method in METHODS,
     * and so on. Call this method before the redo pass, and call finishRedoChecks
     * after the redo pass.
     */
    @SafeVarargs
    private final void setupRedoChecks(Consumer<LogRecord>... methods) {
        for (final Consumer<LogRecord> method : methods) {
            redoMethods.add(record -> {
                method.accept(record);
                LogRecord.onRedoHandler(redoMethods.poll());
            });
        }
        redoMethods.add(record -> fail("LogRecord#redo(recoveryManager, ) called too many times"));
        LogRecord.onRedoHandler(redoMethods.poll());
    }

    /**
     * Helper to finish checks for redo. Call this after the redo pass (or undo
     * pass)- if not enough redo calls were performed, an error is thrown.
     *
     * If setupRedoChecks is used for the redo pass, and this method is not called
     * before the undo pass, and the undo pass calls undo at least once, an error
     * may be incorrectly thrown.
     */
    private void finishRedoChecks() {
        assertTrue("LogRecord#redo() not called enough times", redoMethods.isEmpty());
        LogRecord.onRedoHandler(record -> {
        });
    }

    /**
     * Flushes everything to disk, but does not call RecoveryManager#shutdown.
     * Similar to pulling the plug on the database at a time when no changes are in
     * memory. You can simulate a shutdown where certain changes _are_ in memory, by
     * simply never applying them (i.e. write a log record, but do not make the
     * changes on the buffer manager/disk space manager).
     */
    protected void shutdownRecoveryManager(ARIESRecoveryManager recoveryManager) {
        recoveryManager.logManager.close();
        recoveryManager.bufferManager.evictAll();
        recoveryManager.bufferManager.close();
        recoveryManager.diskSpaceManager.close();
        DummyTransaction.cleanupTransactions();
    }
}
