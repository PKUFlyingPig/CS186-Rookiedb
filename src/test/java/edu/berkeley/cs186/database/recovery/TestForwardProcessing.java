package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.Proj5Part1Tests;
import edu.berkeley.cs186.database.categories.Proj5Tests;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
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

import java.util.*;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

@Category({Proj5Tests.class, Proj5Part1Tests.class})
@SuppressWarnings("serial")
public class TestForwardProcessing {
    private String testDir;
    private RecoveryManager recoveryManager;
    private final Queue<Consumer<LogRecord>> redoMethods = new ArrayDeque<>();

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (2000 * TimeoutScaling.factor)));

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
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
     * Tests transaction abort
     *
     * T1 makes a page update, T2 makes a page update, T1 aborts
     * Checks:
     *  - Transaction table's lastLSN for T1 is the abort record LSN
     *  - T1 and T2's transaction statuses are correct
     */
    @Test
    @Category(PublicTests.class)
    public void testAbort() throws Exception {
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after);

        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after);

        long LSN = recoveryManager.abort(1L);

        assertEquals(LSN, transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.ABORTING, transactionTable.get(1L).transaction.getStatus());
        assertEquals(Transaction.Status.RUNNING, transactionTable.get(2L).transaction.getStatus());
    }

    /**
     * Tests that aborting + ending a transaction correctly undoes its changes
     *
     * This test does the following:
     * 1. Sets up and executes a log w/ 5 log records (see code for record types), the transaction table. T2 completes aborting. T1 aborts but does not end
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
    public void testAbortingEnd() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x00, (byte) 0x00, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        BufferManager bufferManager = getBufferManager(recoveryManager);

        Transaction t1 = DummyTransaction.create(88L);
        Transaction t2 = DummyTransaction.create(455L);

        // 1
        recoveryManager.startTransaction(t1);
        recoveryManager.startTransaction(t2);

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

        updateRecord.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));
        allocRecord.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        bufferManager.evictAll();

        // 10000000001L _not_ in DPT: it should be added on undo
        getTransactionTable(recoveryManager).get(t1.getTransNum()).lastLSN = 40000L;
        getTransactionTable(recoveryManager).get(t1.getTransNum()).touchedPages.add(10000000001L);
        t1.setStatus(Transaction.Status.ABORTING);
        getTransactionTable(recoveryManager).remove(t2.getTransNum());
        t2.setStatus(Transaction.Status.COMPLETE);

        // 2
        LogRecord expectedAllocCLR = allocRecord.undo(40000L).getFirst();
        expectedAllocCLR.setLSN(60000L);
        LogRecord expectedUpdateCLR = updateRecord.undo(60000L).getFirst();
        expectedUpdateCLR.setLSN(70000L);

        setupRedoChecks(Arrays.asList(logRecord -> {
            assertEquals(expectedAllocCLR, logRecord);
        }, logRecord -> {
            assertEquals(expectedUpdateCLR, logRecord);
        }));

        recoveryManager.end(t1.getTransNum());

        finishRedoChecks();

        // 3
        Iterator<LogRecord> iter = logManager.scanFrom(60000L);
        assertEquals(expectedAllocCLR, iter.next());
        LogRecord updateCLR = iter.next();
        assertEquals(expectedUpdateCLR, updateCLR);
        LogRecord expectedEnd = new EndTransactionLogRecord(t1.getTransNum(), updateCLR.getLSN());
        expectedEnd.setLSN(expectedUpdateCLR.getLSN() + expectedUpdateCLR.toBytes().length);
        assertEquals(expectedEnd, iter.next());
        assertFalse(iter.hasNext()); // no other records written

        assertEquals(t1.getStatus(), Transaction.Status.COMPLETE);
        assertEquals(Collections.singletonMap(10000000001L, updateCLR.getLSN()), getDirtyPageTable(recoveryManager));
        assertEquals(Collections.emptyMap(), getTransactionTable(recoveryManager));
        assertEquals(0L, getTransactionCounter(recoveryManager));
    }

    /**
     * Basic test of a transaction updating and committing
     *
     * Tests the following:
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
    public void testSimpleCommit() throws Exception {
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after);
        long LSN1 = recoveryManager.commit(1L);

        assertEquals(LSN1, transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.COMMITTING, transactionTable.get(1L).transaction.getStatus());

        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        long LSN2 = recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after);

        assertTrue(logManager.getFlushedLSN() + " is not greater than or equal to " + LSN1,
                LSN1 <= logManager.getFlushedLSN());
        assertTrue(logManager.getFlushedLSN() + " is not less than " + LSN2, LSN2 > logManager.getFlushedLSN());
    }

    /**
     * Tests functionality of end
     *
     * This test does the following:
     * 1. T1 and T2 log a combination of writes and allocs. T2 commits
     *    Checks:
     *      - LastLSNs in transaction table are updated correctly
     *      - Should be flushed up to the commit record (see LogManager.flushToLSN for more details)
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
    public void testEnd() throws Exception {
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        // 1
        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        long[] LSNs = new long[] { recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 0
                recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after), // 1
                recoveryManager.logPageWrite(1L, pageNum, pageOffset, after, before), // 2
                recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, after, before), // 3
                recoveryManager.logAllocPart(2L, 2), // 4
                recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 5
                recoveryManager.commit(2L), // 6
                recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 7
                -1L, -1L, };

        assertEquals(LSNs[7], transactionTable.get(1L).lastLSN);
        assertEquals(LSNs[6], transactionTable.get(2L).lastLSN);
        assertEquals(LogManager.maxLSN(LogManager.getLSNPage(LSNs[6])), logManager.getFlushedLSN());
        assertEquals(Transaction.Status.COMMITTING, transactionTable.get(2L).transaction.getStatus());

        // 2
        LSNs[8] = recoveryManager.end(2L);
        LSNs[9] = recoveryManager.abort(1L);

        assertEquals(LSNs[9], transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.ABORTING, transactionTable.get(1L).transaction.getStatus());

        // 3
        recoveryManager.end(1L); // 4 CLRs + END

        Iterator<LogRecord> iter = logManager.iterator();

        // CLRs are written correctly
        int totalRecords = 0; // 18 - 3 (master + begin/end chkpt) + 10 (LSNs) + 4 (CLRs) + 1 (END)
        int abort = 0; // 1
        int commit = 0; // 1
        int end = 0; // 2
        int update = 0; // 4 + 2
        int allocPart = 0; // 1
        int undo = 0; // 4
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            totalRecords++;
            switch (record.getType()) {
                case ABORT_TRANSACTION:
                    abort++;
                    break;
                case COMMIT_TRANSACTION:
                    commit++;
                    break;
                case END_TRANSACTION:
                    end++;
                    break;
                case UPDATE_PAGE:
                    update++;
                    break;
                case UNDO_UPDATE_PAGE:
                    undo++;
                    break;
                case ALLOC_PART:
                    allocPart++;
                    break;
                default:
                    break;
            }
        }
        assertEquals(18, totalRecords);
        assertEquals(1, abort);
        assertEquals(1, commit);
        assertEquals(2, end);
        assertEquals(6, update);
        assertEquals(1, allocPart);
        assertEquals(4, undo);

        // Dirty page table
        assertEquals(LSNs[0], (long) dirtyPageTable.get(pageNum));
        assertEquals(LSNs[1], (long) dirtyPageTable.get(pageNum + 1));

        // Transaction table
        assertTrue(transactionTable.isEmpty());

        // Flushed log tail correct
        assertEquals(LogManager.maxLSN(LogManager.getLSNPage(LSNs[6])), logManager.getFlushedLSN());

        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertEquals(Transaction.Status.COMPLETE, transaction2.getStatus());
    }

    /**
     * Tests logging a page update.
     *
     * Does the following:
     * 1. Transaction 1 logs a page update on 10000000002L
     * 2. Transaction 2 logs a page update on 10000000003L
     * 3. Transaction 1 logs another page write on 10000000002L again
     * Checks:
     *  - Transaction table contains this transaction with the correct lastLSN value
     *  - Dirty page table contains page modified by this update with correct recLSN
     *  - TouchedPages contains the modified page
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleLogPageWrite() throws Exception {
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long LSN1 = recoveryManager.logPageWrite(transaction1.getTransNum(), 10000000002L, pageOffset, before, after);

        // Check DPT, X-Act table updates
        assertTrue(transactionTable.containsKey(1L));
        assertEquals(LSN1, transactionTable.get(1L).lastLSN);
        assertTrue(transactionTable.get(1L).touchedPages.contains(10000000002L));
        assertTrue(dirtyPageTable.containsKey(10000000002L));
        assertEquals(LSN1, (long) dirtyPageTable.get(10000000002L));

        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        long LSN2 = recoveryManager.logPageWrite(transaction2.getTransNum(), 10000000003L, pageOffset, before, after);

        assertTrue(transactionTable.containsKey(2L));
        assertEquals(LSN2, transactionTable.get(2L).lastLSN);
        assertTrue(transactionTable.get(2L).touchedPages.contains(10000000003L));
        assertEquals(LSN2, (long) dirtyPageTable.get(10000000003L));

        long LSN3 = recoveryManager.logPageWrite(transaction1.getTransNum(), 10000000002L, pageOffset, before, after);
        assertEquals(LSN3, transactionTable.get(1L).lastLSN);
        assertEquals(LSN1, (long) dirtyPageTable.get(10000000002L));
    }

    /**
     * Test logging large page writes that require two records
     *
     * This test does the following:
     *  - Logs a large page update of size BufferManager.EFFECTIVE_PAGE_SIZE
     *  - Checks:
     *      - That there are two records logged, one undo-only update record and one redo-only update record
     *      - Both are of type UpdatePageLogRecord
     *      - Undo-only record only contains the contents *before* the update (the after field is empty)
     *      - Redo-only record only contains the contents *after* the update (the before field is empty)
     *      - Transaction table and touchedPages updated accordingly
     */
    @Test
    @Category(PublicTests.class)
    public void testTwoPartLogPageWrite() throws Exception {
        long pageNum = 10000000002L;
        byte[] before = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        byte[] after = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        for (int i = 0; i < BufferManager.EFFECTIVE_PAGE_SIZE; ++i) {
            after[i] = (byte) (i % 256);
        }

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long secondLSN = recoveryManager.logPageWrite(transaction1.getTransNum(), pageNum, (short) 0, before, after);
        long firstLSN = secondLSN - 10000L; // previous log page, both at start of a log page

        LogRecord firstLogRecord = logManager.fetchLogRecord(firstLSN);
        LogRecord secondLogRecord = logManager.fetchLogRecord(secondLSN);

        assertTrue(firstLogRecord instanceof UpdatePageLogRecord);
        assertTrue(secondLogRecord instanceof UpdatePageLogRecord);
        assertArrayEquals(before, ((UpdatePageLogRecord) firstLogRecord).before);
        assertArrayEquals(new byte[0], ((UpdatePageLogRecord) secondLogRecord).before);
        assertArrayEquals(new byte[0], ((UpdatePageLogRecord) firstLogRecord).after);
        assertArrayEquals(after, ((UpdatePageLogRecord) secondLogRecord).after);

        assertTrue(transactionTable.containsKey(1L));
        assertTrue(transactionTable.get(1L).touchedPages.contains(pageNum));
    }

    /**
     * Tests rolling back to a savepoint
     *
     * This test does the following:
     * 1. T1 sets a savepoint, logs a page write, then rolls back to the savepoint
     *    Checks:
     *      - Log contains two records: the page update record, and the CLR record for that update
     *      - CLR record contains the correct values (offset, after, transaction number, page number, undoNextLSN)
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleSavepoint() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        recoveryManager.savepoint(1L, "savepoint 1");
        long LSN = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        recoveryManager.rollbackToSavepoint(1L, "savepoint 1");

        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        iter.next(); // page write record

        LogRecord clr = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, clr.getType());

        assertEquals((short) 0, ((UndoUpdatePageLogRecord) clr).offset);
        assertArrayEquals(before, ((UndoUpdatePageLogRecord) clr).after);
        assertEquals(Optional.of(1L), clr.getTransNum());
        assertEquals(Optional.of(10000000001L), clr.getPageNum());
        assertTrue(clr.getUndoNextLSN().orElseThrow(NoSuchElementException::new) < LSN);
    }

    /**
     * Tests basic checkpointing
     *
     *  Does the following:
     *  - T1 logs a write, checkpoints, logs 2 additional writes
     *  - Checks:
     *      - Logs one begin checkpoint record and one end checkpoint record
     *      - Check that the dpt and xact table in the checkpoint record contain the correct contents
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleCheckpoint() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long LSN1 = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        recoveryManager.checkpoint();

        recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, after, before);
        recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        Iterator<LogRecord> iter = logManager.scanFrom(LSN1);
        iter.next(); // page write (LSN 1)

        LogRecord beginCheckpoint = iter.next();
        LogRecord endCheckpoint = iter.next();
        assertEquals(LogType.BEGIN_CHECKPOINT, beginCheckpoint.getType());
        assertEquals(LogType.END_CHECKPOINT, endCheckpoint.getType());

        Map<Long, Pair<Transaction.Status, Long>> txnTable = endCheckpoint.getTransactionTable();
        Map<Long, Long> dpt = endCheckpoint.getDirtyPageTable();
        assertEquals(LSN1, (long) dpt.get(10000000001L));
        assertEquals(new Pair<>(Transaction.Status.RUNNING, LSN1), txnTable.get(1L));
    }

    /**
     * Tests rolling back to a savepoint with records that cause flushes
     *
     * This test does the following:
     * 1. Sets a savepoint at the beginning and a log record with 5 log records (see code for record types)
     * 2. Rolls back to savepoint.
     *    Checks:
     *      - All 5 log records are undone (setupRedoChecks)
     *      - Undoing AllocPartLogRecord and FreePageLogRecord cause flushes
     *      - No additional log records undone
     * 3. Post-rollback checks
     *    Checks:
     *      - CLR records are written to the log
     *      - DPT empty (an UNDO_ALLOC_PAGE (expectedCLR4) which comes after the UNDO_UPDATE_PAGE (expectedCLR3) causes us to flush changes to disk immediately)
     *      - Log manager is flushed (an UNDO_ALLOC_PART record (expectedCLR5) causes us to flush the log)
     *      - Transaction table, transaction status, lastLSN have correct values
     */
    @Test
    @Category(PublicTests.class)
    public void testFlushingRollback() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x00, (byte) 0x00, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        BufferManager bufferManager = getBufferManager(recoveryManager);

        Transaction t1 = DummyTransaction.create(23895734169L);

        // 1
        recoveryManager.startTransaction(t1);

        recoveryManager.savepoint(t1.getTransNum(), "savepoint");

        logManager.flushToLSN(9999L); // force next record to be LSN 10000L
        LogRecord updateRecord1 = new AllocPartLogRecord(t1.getTransNum(), 9175727, 0L);
        logManager.appendToLog(updateRecord1);
        updateRecord1.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(19999L); // force next record to be LSN 20000L
        LogRecord updateRecord2 = new AllocPageLogRecord(t1.getTransNum(), 91757270000008529L, 10000L);
        logManager.appendToLog(updateRecord2);
        updateRecord2.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(29999L); // force next record to be LSN 30000L
        LogRecord updateRecord3 = new UpdatePageLogRecord(t1.getTransNum(), 91757270000008529L, 20000L, (short) 3030,
                before, after);
        logManager.appendToLog(updateRecord3);
        updateRecord3.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(39999L); // force next record to be LSN 40000L
        LogRecord updateRecord4 = new FreePageLogRecord(t1.getTransNum(), 91757270000008529L, 30000L);
        logManager.appendToLog(updateRecord4);
        updateRecord4.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(49999L); // force next record to be LSN 50000L
        LogRecord updateRecord5 = new FreePartLogRecord(t1.getTransNum(), 9175727, 40000L);
        logManager.appendToLog(updateRecord5);
        updateRecord5.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(59999L); // force next record to be LSN 60000L

        bufferManager.evictAll();

        // dpt empty as it should be
        getTransactionTable(recoveryManager).get(t1.getTransNum()).lastLSN = 50000L;
        getTransactionTable(recoveryManager).get(t1.getTransNum()).touchedPages.add(91757270000008529L);
        Map<Long, TransactionTableEntry> expectedTxnTable = new HashMap<>(getTransactionTable(recoveryManager));

        // 2
        LogRecord expectedCLR1 = updateRecord5.undo(50000L).getFirst();
        expectedCLR1.setLSN(60000L);
        LogRecord expectedCLR2 = updateRecord4.undo(60000L).getFirst();
        expectedCLR2.setLSN(70000L);
        LogRecord expectedCLR3 = updateRecord3.undo(70000L).getFirst();
        expectedCLR3.setLSN(80000L);
        LogRecord expectedCLR4 = updateRecord2.undo(80000L).getFirst();
        expectedCLR4.setLSN(expectedCLR3.getLSN() + expectedCLR3.toBytes().length);
        LogRecord expectedCLR5 = updateRecord1.undo(expectedCLR4.getLSN()).getFirst();
        expectedCLR5.setLSN(90000L);

        long initNumIOs = bufferManager.getNumIOs();

        setupRedoChecks(Arrays.asList(logRecord -> {
            assertEquals(expectedCLR1, logRecord);
            assertEquals(expectedCLR1.getLSN(), logRecord.getLSN());
            assertEquals(69999L, logManager.getFlushedLSN());
        }, logRecord -> {
            assertEquals(expectedCLR2, logRecord);
            assertEquals(expectedCLR2.getLSN(), logRecord.getLSN());
            assertEquals(79999L, logManager.getFlushedLSN());
        }, logRecord -> {
            assertEquals(expectedCLR3, logRecord);
            assertEquals(expectedCLR3.getLSN(), logRecord.getLSN());
            assertEquals(79999L, logManager.getFlushedLSN());
        }, logRecord -> {
            assertEquals(expectedCLR4, logRecord);
            assertEquals(expectedCLR4.getLSN(), logRecord.getLSN());
            assertEquals(89999L, logManager.getFlushedLSN());
        }, logRecord -> {
            assertEquals(expectedCLR5, logRecord);
            assertEquals(expectedCLR5.getLSN(), logRecord.getLSN());
            assertEquals(99999L, logManager.getFlushedLSN());
        }));

        recoveryManager.rollbackToSavepoint(t1.getTransNum(), "savepoint");

        finishRedoChecks();

        // 3
        long finalNumIOs = bufferManager.getNumIOs();
        // 4 new log pages (8 w/ flush) + reading 5 records (alloc/free aren't counted
        // by the i/o counter)
        // + writing one page
        assertEquals(14, finalNumIOs - initNumIOs);

        Iterator<LogRecord> iter = logManager.scanFrom(60000L);
        assertEquals(expectedCLR1, iter.next());
        assertEquals(expectedCLR2, iter.next());
        assertEquals(expectedCLR3, iter.next());
        assertEquals(expectedCLR4, iter.next());
        assertEquals(expectedCLR5, iter.next());
        assertFalse(iter.hasNext()); // no other records written

        assertEquals(99999L, logManager.getFlushedLSN()); // flushed
        assertEquals(t1.getStatus(), Transaction.Status.RUNNING);
        assertEquals(Collections.emptyMap(), getDirtyPageTable(recoveryManager));
        expectedTxnTable.get(t1.getTransNum()).lastLSN = 90000L;
        assertEquals(expectedTxnTable, getTransactionTable(recoveryManager));
        assertEquals(0L, getTransactionCounter(recoveryManager));
    }

    /**
     * Test rolling back T2 while T1 is also running
     *
     * Does the following:
     * 1. T1 writes, T2 writes, set savepoint, T1 and T2 continue writing
     * 2. Rolls back to savepoint
     *    Checks:
     *      - Nothing undone (all records for T2 after the savepoint have already been undone)
     *      - Checks that DPT and Xact table remain intact
     *      - T1 still running
     */
    @Test
    @Category(PublicTests.class)
    public void testNestedRollback() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x00, (byte) 0x00, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        BufferManager bufferManager = getBufferManager(recoveryManager);

        Transaction t1 = DummyTransaction.create(23895732L);
        Transaction t2 = DummyTransaction.create(9823758L);

        // 1
        recoveryManager.startTransaction(t1);
        recoveryManager.startTransaction(t2);

        logManager.flushToLSN(9999L); // force next record to be LSN 10000L
        LogRecord updateRecord1 = new UpdatePageLogRecord(t1.getTransNum(), 10000000001L, 0L, (short) 71,
                before, after);
        logManager.appendToLog(updateRecord1);
        updateRecord1.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(19999L); // force next record to be LSN 20000L
        LogRecord updateRecord2 = new UpdatePageLogRecord(t2.getTransNum(), 10000000001L, 0L, (short) 33,
                before, after);
        logManager.appendToLog(updateRecord2);
        updateRecord2.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        getTransactionTable(recoveryManager).get(t2.getTransNum()).lastLSN = updateRecord2.getLSN();
        recoveryManager.savepoint(t2.getTransNum(), "savepoint");

        logManager.flushToLSN(29999L); // force next record to be LSN 30000L
        LogRecord updateRecord3 = new UpdatePageLogRecord(t2.getTransNum(), 10000000001L, 20000L,
                (short) 11,
                before, after);
        logManager.appendToLog(updateRecord3);
        updateRecord3.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(39999L); // force next record to be LSN 40000L
        LogRecord updateRecord4 = new UpdatePageLogRecord(t1.getTransNum(), 10000000002L, 10000L,
                (short) 991,
                before, after);
        logManager.appendToLog(updateRecord4);
        updateRecord4.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(49999L); // force next record to be LSN 50000L
        LogRecord updateRecord5 = new UndoUpdatePageLogRecord(t2.getTransNum(), 10000000001L, 30000L,
                20000L,
                (short) 11, before);
        logManager.appendToLog(updateRecord5);
        updateRecord5.redo(getDiskSpaceManager(recoveryManager), getBufferManager(recoveryManager));

        logManager.flushToLSN(59999L); // force next record to be LSN 60000L

        bufferManager.evictAll();

        getDirtyPageTable(recoveryManager).put(10000000001L, 10000L);
        getDirtyPageTable(recoveryManager).put(10000000003L, 40000L);
        getTransactionTable(recoveryManager).get(t1.getTransNum()).lastLSN = 40000L;
        getTransactionTable(recoveryManager).get(t1.getTransNum()).touchedPages.add(10000000001L);
        getTransactionTable(recoveryManager).get(t1.getTransNum()).touchedPages.add(10000000002L);
        getTransactionTable(recoveryManager).get(t2.getTransNum()).lastLSN = 50000L;
        getTransactionTable(recoveryManager).get(t2.getTransNum()).touchedPages.add(10000000001L);
        Map<Long, Long> expectedDPT = new HashMap<>(getDirtyPageTable(recoveryManager));
        Map<Long, TransactionTableEntry> expectedTxnTable = new HashMap<>(getTransactionTable(
                    recoveryManager));

        long initNumIOs = bufferManager.getNumIOs();

        setupRedoChecks(Collections.emptyList());

        recoveryManager.rollbackToSavepoint(t2.getTransNum(), "savepoint");

        finishRedoChecks();

        long finalNumIOs = bufferManager.getNumIOs();
        // read CLR (1)
        assertEquals(1L, finalNumIOs - initNumIOs);

        // 2
        Iterator<LogRecord> iter = logManager.scanFrom(60000L);
        assertFalse(iter.hasNext()); // no other records written
        assertEquals(59999L, logManager.getFlushedLSN()); // not flushed
        assertEquals(t1.getStatus(), Transaction.Status.RUNNING);
        assertEquals(expectedDPT, getDirtyPageTable(recoveryManager));
        assertEquals(expectedTxnTable, getTransactionTable(recoveryManager));
        assertEquals(0L, getTransactionCounter(recoveryManager));
    }

    /*************************************************************************
     * Helpers - these are similar to the ones available in TestARIESStudent *
     *************************************************************************/

    /**
     * Helper to set up checks for redo. The first call to LogRecord.redo will call
     * the first method in METHODS, the second call to the second method in METHODS,
     * and so on. Call this method before the redo pass, and call finishRedoChecks
     * after the redo pass.
     */
    private void setupRedoChecks(Collection<Consumer<LogRecord>> methods) {
        for (final Consumer<LogRecord> method : methods) {
            redoMethods.add(record -> {
                method.accept(record);
                LogRecord.onRedoHandler(redoMethods.poll());
            });
        }
        redoMethods.add(record -> fail("LogRecord#redo() called too many times"));
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
     * Loads the recovery manager from disk.
     *
     * @param dir testDir
     * @return recovery manager, loaded from disk
     */
    protected RecoveryManager loadRecoveryManager(String dir) throws Exception {
        RecoveryManager recoveryManager = new ARIESRecoveryManagerNoLocking(
                new DummyLockContext("database"), DummyTransaction::create);
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
        return recoveryManager;
    }

    protected BufferManager getBufferManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).bufferManager;
    }

    protected DiskSpaceManager getDiskSpaceManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).diskSpaceManager;
    }

    protected LogManager getLogManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).logManager;
    }

    protected List<String> getLockRequests(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).lockRequests;
    }

    protected long getTransactionCounter(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManagerNoLocking) recoveryManager).transactionCounter;
    }

    protected Map<Long, Long> getDirtyPageTable(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).dirtyPageTable;
    }

    protected Map<Long, TransactionTableEntry> getTransactionTable(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).transactionTable;
    }
}
