package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.Proj5Part2Tests;
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

@Category({Proj5Tests.class, Proj5Part2Tests.class})
@SuppressWarnings("serial")
public class TestRestartRecovery {
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
     * Test analysis phase of recovery
     *
     * Does the following:
     * 1. Sets up log (see comments below for table)
     * 2. Simulates database shutdown
     * 3. Runs analysis phase of recovery
     * 4. Checks for correct transaction table and DPT (see comments below for tables)
     * 5. Checks:
     *      - Transaction statuses/cleanup statuses
     *      - Transactions requested X locks for all page-related records
     *      - TransactionCounter updated for beginCheckpoint
     */
    @Test
    @Category(PublicTests.class)
    public void testRestartAnalysis() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);
        DummyTransaction transaction2 = DummyTransaction.create(2L);
        DummyTransaction transaction3 = DummyTransaction.create(3L);

        // 1
        // The code below sets up the following log (LSNs in this table are not the same as the code and are used for simplicity)
        //       LSN |    Xact |   prevLSN |     pageNum  |             Type
        // ----------+---------+-----------+--------------+------------------
        //        10 |       1 |         0 | 10000000001L |           Update
        //        20 |       1 |        10 | 10000000002L |           Update
        //        30 |       3 |         0 | 10000000003L |           Update
        //        40 |       1 |        20 |              |           Commit
        //        50 |       1 |        40 |              |              End
        //        60 |       2 |         0 | 10000000001L |        Free Page
        //        70 |       2 |        60 |              |            Abort
        //        80 |         |           |              | Begin Checkpoint
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
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord(9876543210L))); // 7

        // 2
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // new recovery manager - tables/log manager/other state loaded with old manager
        // are different
        // with the new recovery manager
        logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        List<String> lockRequests = getLockRequests(recoveryManager);

        // 3
        runAnalysis(recoveryManager);

        // 4
        // Xact table after running analysis (reminder that LSNs do not reflect the LSNs in the code)
        //      Xact | lastLSN | touchedPages
        // ----------+---------+--------------
        //         2 |      70 | 10000000001L
        //         3 |       * | 10000000003L
        // * indicates the LSN of the abort record that was added for Xact 3 at the end of analysis
        assertFalse(transactionTable.containsKey(1L));
        assertTrue(transactionTable.containsKey(2L));
        assertEquals((long) LSNs.get(6), transactionTable.get(2L).lastLSN);
        assertEquals(new HashSet<>(Collections.singletonList(10000000001L)), transactionTable.get(2L).touchedPages);
        assertTrue(transactionTable.containsKey(3L));
        assertTrue(transactionTable.get(3L).lastLSN > LSNs.get(7));
        assertEquals(new HashSet<>(Collections.singletonList(10000000003L)), transactionTable.get(3L).touchedPages);

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

        // 5
        // status/cleanup
        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertTrue(transaction1.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction2.getStatus());
        assertFalse(transaction2.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction3.getStatus());
        assertFalse(transaction2.cleanedUp);

        // lock requests made
        assertEquals(Arrays.asList("request 1 X(database/1/10000000001)", "request 1 X(database/1/10000000002)",
                "request 3 X(database/1/10000000003)", "request 2 X(database/1/10000000001)"), lockRequests);

        // transaction counter - from begin checkpoint
        assertEquals(9876543210L, getTransactionCounter(recoveryManager));

        // FlushedLSN
        assertEquals(LogManager.maxLSN(LogManager.getLSNPage(LSNs.get(7))), logManager.getFlushedLSN());
    }

    @Test
    @Category(PublicTests.class)
    public void testAnalysisCheckpoints() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, 0L))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(2L, 10000000004L, 0L, (short) 0, before, after))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(2L, 10000000006L, LSNs.get(1), (short) 0, before, after))); // 2
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(3L, 10000000007L, 0L, (short) 0, before, after))); // 3
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(3L, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord(0xf00dbaadL))); // 5
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord(0xf00dbaaeL))); // 6
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(2L, 10000000005L, LSNs.get(2), (short) 0, before, after))); // 7
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(2L, 10000000003L, LSNs.get(7), (short) 0, before, after))); // 8
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(2L, 10000000003L, LSNs.get(8), (short) 0, after, before))); // 9
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(2L, 10000000006L, LSNs.get(9), (short) 0, after, before))); // 10
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(0)))); // 11
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(2L, LSNs.get(10)))); // 12
        LSNs.add(logManager.appendToLog(logManager.fetchLogRecord(LSNs.get(3)).undo(LSNs.get(3)).getFirst())); // 13
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(6L, 0L))); // 14
        LSNs.add(logManager.appendToLog(new EndCheckpointLogRecord(new HashMap<Long, Long>() {
            {
                put(10000000003L, LSNs.get(9));
                put(10000000006L, LSNs.get(2));
            }
        }, new HashMap<Long, Pair<Transaction.Status, Long>>() {
            {
                put(3L, new Pair<>(Transaction.Status.ABORTING, LSNs.get(4)));
                put(6L, new Pair<>(Transaction.Status.RUNNING, 0L));
            }
        }, Collections.emptyMap()))); // 15
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(5L, 10000000001L, 0L, (short) 0, before, after))); // 16
        LSNs.add(logManager.appendToLog(
                new EndCheckpointLogRecord(Collections.emptyMap(), new HashMap<Long, Pair<Transaction.Status, Long>>() {
                    {
                        put(1L, new Pair<>(Transaction.Status.COMMITTING, LSNs.get(0)));
                        put(2L, new Pair<>(Transaction.Status.COMMITTING, LSNs.get(12)));
                        put(4L, new Pair<>(Transaction.Status.RUNNING, 0L));
                    }
                }, new HashMap<Long, List<Long>>() {
                    {
                        put(2L, Arrays.asList(10000000003L, 10000000004L, 10000000005L, 10000000006L));
                        put(3L, Collections.singletonList(10000000007L));
                    }
                }))); // 17
        LSNs.add(logManager.appendToLog(new EndCheckpointLogRecord(new HashMap<Long, Long>() {
            {
                put(10000000003L, LSNs.get(9));
                put(10000000006L, LSNs.get(2));
            }
        }, new HashMap<Long, Pair<Transaction.Status, Long>>() {
            {
                put(3L, new Pair<>(Transaction.Status.ABORTING, LSNs.get(4)));
                put(6L, new Pair<>(Transaction.Status.RUNNING, 0L));
            }
        }, Collections.emptyMap()))); // 18
        // end/abort records from analysis
        LSNs.add(10000L); // 19, new page
        LSNs.add(LSNs.get(19) + (new EndTransactionLogRecord(0L, 0L)).toBytes().length); // 20
        LSNs.add(LSNs.get(20) + (new AbortTransactionLogRecord(0L, 0L)).toBytes().length); // 21
        LSNs.add(LSNs.get(21) + (new AbortTransactionLogRecord(0L, 0L)).toBytes().length); // 22
        logManager.rewriteMasterRecord(new MasterLogRecord(LSNs.get(5)));

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // new recovery manager - tables/log manager/other state loaded with old manager
        // are different
        // with the new recovery manager
        logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        List<String> lockRequests = getLockRequests(recoveryManager);

        runAnalysis(recoveryManager);

        // check log
        Iterator<LogRecord> iter = logManager.scanFrom(10000L);
        assertEquals(new EndTransactionLogRecord(2L, LSNs.get(12)), iter.next());
        assertEquals(new AbortTransactionLogRecord(4L, 0L), iter.next());
        assertEquals(new AbortTransactionLogRecord(5L, LSNs.get(16)), iter.next());
        assertEquals(new EndTransactionLogRecord(6L, LSNs.get(14)), iter.next());
        assertFalse(iter.hasNext());

        assertEquals(0xf00dbaaeL, getTransactionCounter(recoveryManager));
        assertEquals(9999L, logManager.getFlushedLSN());
    }

    /**
     * Test redo phase of recovery
     *
     * Does the following:
     * 1. Sets up log. Transaction 1 makes a few updates/allocs, commits, and ends.
     * 2. Simulate database shutdown and sets up dpt (to simulate analysis)
     * 3. Runs redo phase and checks that we only redo records 2 - 4 and no others (since 0 - 1 were already flushed to disk according to dpt)
     */
    @Test
    @Category(PublicTests.class)
    @SuppressWarnings("unused")
    public void testRestartRedo() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction t1 = DummyTransaction.create(1L);

        // 1
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
        logManager.fetchLogRecord(LSNs.get(0)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(dsm, bm);

        // 2
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up dirty page table - xact table is empty (transaction ended)
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        dirtyPageTable.put(10000000002L, LSNs.get(2));
        dirtyPageTable.put(10000000003L, LSNs.get(3));

        // 3
        // set up checks for redo - these get called in sequence with each
        // LogRecord#redo call
        setupRedoChecks(Arrays.asList((LogRecord record) -> assertEquals((long) LSNs.get(2), (long) record.LSN),
                (LogRecord record) -> assertEquals((long) LSNs.get(3), (long) record.LSN),
                (LogRecord record) -> assertEquals((long) LSNs.get(4), (long) record.LSN)));

        runRedo(recoveryManager);

        finishRedoChecks();
    }

    /**
     * Test undo phase of recovery
     *
     * Does the following:
     * 1. Sets up log - T1 makes 4 updates and then aborts.
     * 2. We execute the changes specified in log records, simulate a db shutdown, and set up Xact table to simulate analysis phase
     * 3. Runs the undo phase
     *    Checks:
     *      - The 4 update records are undone and appended to log
     *      - Transaction table is updated accordingly as we undo
     *      - No other records are undone
     *      - T1 status set to COMPLETE
     */
    @Test
    @Category(PublicTests.class)
    public void testRestartUndo() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);

        // 1
        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1, before, after))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(1), (short) 2, before, after))); // 2
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000004L, LSNs.get(2), (short) 3, before, after))); // 3
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(3)))); // 4

        // 2
        // actually do the writes
        for (int i = 0; i < 4; ++i) {
            logManager.fetchLogRecord(LSNs.get(i)).redo(dsm, bm);
        }

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up xact table - leaving DPT empty
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        TransactionTableEntry entry1 = new TransactionTableEntry(transaction1);
        entry1.lastLSN = LSNs.get(4);
        entry1.touchedPages = new HashSet<>(Arrays.asList(10000000001L, 10000000002L, 10000000003L, 10000000004L));
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // 3
        // set up checks for undo - these get called in sequence with each
        // LogRecord#redo call
        // (which should be called on CLRs)
        setupRedoChecks(Arrays.asList((LogRecord record) -> {
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
        }));

        runUndo(recoveryManager);

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
    public void testUndoCLR() throws Exception { // Releasing public sp20
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        // 1
        DummyTransaction t1 = DummyTransaction.create(1L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 0, before, after))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(1), (short) 0, before, after))); // 2
        LSNs.add(logManager.appendToLog(logManager.fetchLogRecord(LSNs.get(2)).undo(LSNs.get(2)).getFirst())); // 3
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(logManager.fetchLogRecord(LSNs.get(1)).undo(LSNs.get(4)).getFirst())); // 5

        logManager.fetchLogRecord(LSNs.get(0)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(2)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(3)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(5)).redo(dsm, bm);

        // 2
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);
        logManager = getLogManager(recoveryManager);

        // 3
        // set up xact table - leaving DPT empty
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        TransactionTableEntry entry1 = new TransactionTableEntry(t1);
        entry1.lastLSN = LSNs.get(5);
        entry1.touchedPages = new HashSet<>(Arrays.asList(10000000001L, 10000000002L, 10000000003L));
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // set up checks for undo - these get called in sequence with each
        // LogRecord#redo call
        // (which should be called on CLRs)
        setupRedoChecks(Collections.singletonList((LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000001L), record.getPageNum());
        }));

        runUndo(recoveryManager);

        finishRedoChecks();

        // 4
        assertEquals(Transaction.Status.COMPLETE, t1.getStatus());
        assertFalse(transactionTable.containsKey(1L));

        Iterator<LogRecord> iter = logManager.scanFrom(10000L);

        LogRecord next = iter.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(0)).undo(LSNs.get(5)).getFirst(), next);
        long lastLSN = next.LSN;

        assertEquals(new EndTransactionLogRecord(1L, lastLSN), iter.next());

        assertFalse(iter.hasNext());
    }

    /**
     * Tests that restartUndo properly undoes log records while flushing the log and updating the DPT accordingly
     *
     * This test does the following:
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
    public void testUndoDPTAndFlush() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction t1 = DummyTransaction.create(1L);

        // 1
        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before, after))); // 0
        LSNs.add(logManager.appendToLog(new AllocPageLogRecord(1L, 10000000099L, LSNs.get(0)))); // 1
        LSNs.add(logManager
                .appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(1), (short) 0, before, after))); // 2
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(2)))); // 3

        logManager.fetchLogRecord(LSNs.get(0)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(2)).redo(dsm, bm);

        // 2
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);
        logManager = getLogManager(recoveryManager);

        // set up xact table - leaving DPT empty
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        TransactionTableEntry entry1 = new TransactionTableEntry(t1);
        entry1.lastLSN = LSNs.get(3);
        entry1.touchedPages = new HashSet<>(Arrays.asList(10000000001L, 10000000002L, 10000000099L));
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // 3
        // set up checks for undo - these get called in sequence with each
        // LogRecord#redo call
        // (which should be called on CLRs)
        LogManager logManager1 = logManager;
        setupRedoChecks(Arrays.asList((LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000002L), record.getPageNum());
        }, (LogRecord record) -> {
            assertEquals(LogType.UNDO_ALLOC_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals(19999L, logManager1.getFlushedLSN()); // flushed
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000099L), record.getPageNum());
        }, (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000001L), record.getPageNum());
        }));

        runUndo(recoveryManager);

        // 4
        finishRedoChecks();

        assertEquals(Transaction.Status.COMPLETE, t1.getStatus());
        assertFalse(transactionTable.containsKey(1L));

        // 5
        Iterator<LogRecord> iter = logManager.scanFrom(10000L);

        LogRecord next = iter.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(2)).undo(LSNs.get(3)).getFirst(), next);
        long LSN1 = next.LSN;

        next = iter.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(1)).undo(LSN1).getFirst(), next);
        long LSN2 = next.LSN;

        next = iter.next();
        assertEquals(logManager.fetchLogRecord(LSNs.get(0)).undo(LSN2).getFirst(), next);
        long LSN3 = next.LSN;

        assertEquals(new EndTransactionLogRecord(1L, LSN3), iter.next());

        assertFalse(iter.hasNext());

        assertEquals(new HashMap<Long, Long>() {
            {
                put(10000000001L, LSN3);
                put(10000000002L, LSN1);
            }
        }, getDirtyPageTable(recoveryManager));
    }

    /**
     * Tests simple case of recovery in its entirety (analysis, redo, undo)
     *
     * Does the following
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
    public void testSimpleRestart() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);
        // 1
        recoveryManager.startTransaction(transaction1);
        long LSN = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        // 2
        setupRedoChecks(Collections.singletonList((LogRecord record) -> {
            assertEquals(LSN, record.getLSN());
            assertEquals(LogType.UPDATE_PAGE, record.getType());
        }));

        Runnable func = recoveryManager.restart(); // analysis + redo

        finishRedoChecks();

        assertTrue(transactionTable.containsKey(transaction1.getTransNum()));
        TransactionTableEntry entry = transactionTable.get(transaction1.getTransNum());
        assertEquals(Transaction.Status.RECOVERY_ABORTING, entry.transaction.getStatus());
        assertEquals(new HashSet<>(Collections.singletonList(10000000001L)), entry.touchedPages);
        assertEquals(LSN, (long) dirtyPageTable.get(10000000001L));

        // 3
        func.run(); // undo

        Iterator<LogRecord> iter = logManager.scanFrom(LSN);

        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UNDO_UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertFalse(iter.hasNext());
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
    public void testRestart() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        // 1
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

        // 2
        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        LogManager logManager = getLogManager(recoveryManager);

        // 3
        recoveryManager.restart().run(); // run everything in restart recovery

        Iterator<LogRecord> iter = logManager.iterator();
        assertEquals(LogType.MASTER, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.COMMIT_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, iter.next().getType());

        LogRecord record = iter.next();
        assertEquals(LogType.ABORT_TRANSACTION, record.getType()); // T3
        long LSN8 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 6 (T3's write)
        assertEquals(LSN8, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[3], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN9 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 4 (T2's write)
        assertEquals(LSNs[7], (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[1], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN10 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 3 (T3's write)
        assertEquals(LSN9, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LogType.END_TRANSACTION, iter.next().getType()); // End record for T3

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType()); // Undo 1 (T2's write)
        assertEquals(LSN10, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));

        assertEquals(LogType.END_TRANSACTION, iter.next().getType()); // End record for T2
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertFalse(iter.hasNext());
    }

    /**
     * Tests that DPT is cleaned up correctly in restart as indicated in the spec.
     * Should only contain pages that are actually dirty.
     */
    @Test
    @Category(PublicTests.class)
    public void testRestartCleanup() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);

        recoveryManager.startTransaction(transaction1);
        long LSN1 = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);
        long LSN2 = recoveryManager.logPageWrite(1L, 10000000002L, (short) 0, before, after);

        getLogManager(recoveryManager).fetchLogRecord(LSN1).redo(getDiskSpaceManager(recoveryManager),
                getBufferManager(recoveryManager));

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        recoveryManager.restart(); // analysis + redo + clean DPT

        assertEquals(Collections.singletonMap(10000000002L, LSN2), getDirtyPageTable(recoveryManager));
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

    /**
     * Flushes everything to disk, but does not call RecoveryManager#shutdown.
     * Similar to pulling the plug on the database at a time when no changes are in
     * memory. You can simulate a shutdown where certain changes _are_ in memory, by
     * simply never applying them (i.e. write a log record, but do not make the
     * changes on the buffer manager/disk space manager).
     */
    protected void shutdownRecoveryManager(RecoveryManager recoveryManager) throws Exception {
        ARIESRecoveryManager arm = (ARIESRecoveryManager) recoveryManager;
        arm.logManager.close();
        arm.bufferManager.evictAll();
        arm.bufferManager.close();
        arm.diskSpaceManager.close();
        DummyTransaction.cleanupTransactions();
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

    protected void runAnalysis(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartAnalysis();
    }

    protected void runRedo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartRedo();
    }

    protected void runUndo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartUndo();
    }
}
