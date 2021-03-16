package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@Category({Proj4Tests.class, Proj4Part1Tests.class})
public class TestLockManager {
    private LoggingLockManager lockman;
    private TransactionContext[] transactions;
    private ResourceName dbResource;
    private ResourceName[] tables;

    // 2 seconds per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                2000 * TimeoutScaling.factor)));

    /**
     * Given a LockManager lockman, checks if transaction holds a Lock specified
     * by type on the resource specified by name
     */
    static boolean holds(LockManager lockman, TransactionContext transaction, ResourceName name,
                         LockType type) {
        List<Lock> locks = lockman.getLocks(transaction);
        if (locks == null) {
            return false;
        }
        for (Lock lock : locks) {
            if (lock.name == name && lock.lockType == type) {
                return true;
            }
        }
        return false;
    }

    @Before
    public void setUp() {
        // Sets up a LockManager, 8 possible transactions, a database resource
        // and 8 possible tables for use in tests
        lockman = new LoggingLockManager();
        transactions = new TransactionContext[8];
        dbResource = new ResourceName("database");
        tables = new ResourceName[transactions.length];
        for (int i = 0; i < transactions.length; ++i) {
            transactions[i] = new DummyTransactionContext(lockman, i);
            tables[i] = new ResourceName(dbResource,"table" + i);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLock() {
        /**
         * Transaction 0 acquires an S lock on table0
         * Transaction 1 acquires an X lock on table1
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(transactions[0], tables[0], LockType.S));
        runner.run(1, () -> lockman.acquire(transactions[1], tables[1], LockType.X));

        // Transaction 0 should have an S lock on table0
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[0]));

        // table0 should only have an S lock from Transaction 0
        List<Lock> expectedTable0Locks = Collections.singletonList(new Lock(tables[0], LockType.S, 0L));
        assertEquals(expectedTable0Locks, lockman.getLocks(tables[0]));

        // Transaction 1 should have an X lock on table1
        assertEquals(LockType.X, lockman.getLockType(transactions[1], tables[1]));

        // table1 should only have an X lock from Transaction 1
        List<Lock>expectedTable1Locks = Collections.singletonList(new Lock(tables[1], LockType.X, 1L));
        assertEquals(expectedTable1Locks, lockman.getLocks(tables[1]));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLockFail() {
        DeterministicRunner runner = new DeterministicRunner(1);
        TransactionContext t0 = transactions[0];

        // Transaction 0 acquires an X lock on dbResource
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
        try {
            // Transaction 0 attempts to acquire another X lock on dbResource
            runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
            fail("Attempting to acquire a duplicate lock should throw a " +
                    "DuplicateLockRequestException.");
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleReleaseLock() {
        /**
         * Transaction 0 acquires an X lock on dbResource
         * Transaction 0 releases its lock on dbResource
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquire(transactions[0], dbResource, LockType.X);
            lockman.release(transactions[0], dbResource);
        });

        // Transaction 0 should have no lock on dbResource
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));

        // There should be no locks on dbResource
        assertEquals(Collections.emptyList(), lockman.getLocks(dbResource));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testReleaseUnheldLock() {
        DeterministicRunner runner = new DeterministicRunner(1);

        TransactionContext t1 = transactions[0];
        try {
            runner.run(0, () -> lockman.release(t1, dbResource));
            fail("Releasing a lock on a resource you don't hold a lock on " +
                    "should throw a NoLockHeldException");
        } catch (NoLockHeldException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleConflict() {
        /**
         * Transaction 0 acquires an X lock on dbResource
         * Transaction 1 attempts to acquire an X lock on dbResource but
         *   blocks due to a conflict with Transaction 0's X lock
         *
         * After this:
         *   Transaction 0 should have an X lock on dbResource
         *   Transaction 1 should have no lock on dbResource
         *   Transaction 0 should not be blocked
         *   Transaction 1 should be blocked (waiting to acquire an X lock on dbResource)
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));

        // Lock checks
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.NL, lockman.getLockType(transactions[1], dbResource));
        List<Lock> expectedDbLocks = Collections.singletonList(new Lock(dbResource, LockType.X, 0L));
        assertEquals(expectedDbLocks, lockman.getLocks(dbResource));

        // Block checks
        assertFalse(transactions[0].getBlocked());
        assertTrue(transactions[1].getBlocked());

        /**
         * Transaction 0 releases its lock on dbResource
         * Transaction 1 should unblock, and acquire an X lock on dbResource
         *
         * After this:
         *   Transaction 0 should have no lock on dbResource
         *   Transaction 1 should have an X lock dbResource
         *   Both transactions should be unblocked
         *
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // Lock checks
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.X, lockman.getLockType(transactions[1], dbResource));
        List<Lock> expectedDbLocks2 = Collections.singletonList(new Lock(dbResource, LockType.X, 1L));
        assertEquals(expectedDbLocks2, lockman.getLocks(dbResource));

        // Block checks
        assertFalse(transactions[0].getBlocked());
        assertFalse(transactions[1].getBlocked());

        runner.joinAll();
    }

    // Tests below here are not required for the Spring 2021 Project 4 Part 1 deadline.

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireRelease() {
        /**
         * Transaction 0 acquires an S lock on table0
         * Transaction 0 acquires an S lock on table1 and releases its lock on table0
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Collections.emptyList());
            lockman.acquireAndRelease(transactions[0], tables[1], LockType.S, Collections.singletonList(tables[0]));
        });

        // Transaction 0 should have no lock on table0
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], tables[0]));

        // table0 should have no locks on it from any transaction
        assertEquals(Collections.emptyList(), lockman.getLocks(tables[0]));

        // Transaction 0 should have an S lock on table1
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[1]));

        // table1 should only have an S lock from Transaction 0
        List<Lock> expectedTable1Locks = Collections.singletonList(new Lock(tables[1], LockType.S, 0L));
        assertEquals(expectedTable1Locks, lockman.getLocks(tables[1]));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseQueue() {
        /**
         * Transaction 0 acquires an X lock on table0
         * Transaction 1 acquires an X lock on table1
         * Transaction 0 attempts to acquire an X lock on table1 and
         *     release its X lock on table0
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        runner.run(1, () -> lockman.acquireAndRelease(transactions[1], tables[1], LockType.X,
                   Collections.emptyList()));
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[1], LockType.X,
                   Collections.singletonList(tables[0])));

        // Transaction 0 should have an X lock on table0
        assertEquals(LockType.X, lockman.getLockType(transactions[0], tables[0]));

        // table0 should only have have an X lock from Transaction 0
        List<Lock> expectedTable0Locks = Collections.singletonList(new Lock(tables[0], LockType.X, 0L));
        assertEquals(expectedTable0Locks, lockman.getLocks(tables[0]));

        // Transaction 0 should have no lock on table1
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], tables[1]));

        // table1 should only have an X lock from Transaction 1
        List<Lock> expectedTable1Locks = Collections.singletonList(new Lock(tables[1], LockType.X, 1L));
        assertEquals(expectedTable1Locks, lockman.getLocks(tables[1]));

        // Transaction 0 should still be blocked waiting to acquire an X lock on table1
        assertTrue(transactions[0].getBlocked());

        runner.join(1);
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseDuplicateLock() {
        // Transaction 0 acquires an X lock on table0
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        try {
            // Transaction 0 attempts to acquire another X lock on table0
            runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                       Collections.emptyList()));
            fail("Attempting to acquire a duplicate lock should throw a " +
                 "DuplicateLockRequestException.");
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseNotHeld() {
        // Transaction 0 acquires an X lock on table0
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        try {
            // Transaction 0 attempts to acquire an X lock on table2,
            // and release locks on table0 and table1.
            runner.run(0, () -> lockman.acquireAndRelease(
                transactions[0], tables[2],
                LockType.X, Arrays.asList(tables[0], tables[1]))
            );
            fail("Attempting to release a lock that is not held should throw " +
                 "a NoLockHeldException.");
        } catch (NoLockHeldException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseUpgrade() {
        /**
         * Transaction 0 acquires an S lock on table0
         * Transaction 0 acquires an X lock on table0 and releases its S lock
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Collections.emptyList());
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                                      Collections.singletonList(tables[0]));
        });

        // Transaction 0 should have an X lock on table0
        assertEquals(LockType.X, lockman.getLockType(transactions[0], tables[0]));

        // table0 should only have an X lock from Transaction 0
        List<Lock> expectedTable0Locks = Collections.singletonList(new Lock(tables[0], LockType.X, 0L));
        assertEquals(expectedTable0Locks, lockman.getLocks(tables[0]));

        // Transaction 0 should not be blocked
        assertFalse(transactions[0].getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSXS() {
        /**
         * Transaction 0 acquires an S lock on dbResource
         * Transaction 1 attempts to acquire an X lock on dbResource but
         *    blocks due to a conflict with Transaction 0's S lock
         * Transaction 2 attempts to acquire an S lock on dbResource but
         *    blocks due to existing queue
         *
         * After this:
         *    dbResource should have an S lock from Transaction 0
         *    dbResource should have [X (T1), S (T2)] in its queue
         *    Transaction 0 should be unblocked
         *    Transaction 1 should be blocked
         *    Transaction 2 should be blocked
         */
        DeterministicRunner runner = new DeterministicRunner(3);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.S));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.S));

        // Lock check
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 0L)),
                     lockman.getLocks(dbResource));

        // Block checks
        List<Boolean> blocked_status = new ArrayList<>();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, true, true), blocked_status);

        /**
         * Transaction 0 releases its S lock on dbResource
         * Transaction 1 should unblock and acquire an X lock on dbResource
         * Transaction 2 will move forward in the queue, but still be blocked
         *
         * After this:
         *    dbResource should have an X lock from Transaction 1
         *    dbResource should have [S (T2)] in its queue
         *    Transaction 0 should be unblocked
         *    Transaction 1 should be unblocked
         *    Transaction 2 should be blocked
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // Lock check
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 1L)),
                     lockman.getLocks(dbResource));

        // Block checks
        blocked_status.clear();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, true), blocked_status);

        /**
         * Transaction 1 releases its X lock on dbResource
         * Transaction 2 should unblock and acquire an S lock on dbResource
         *
         * After this:
         *    dbResource should have an S lock from Transaction 1
         *    All transactions should be unblocked
         */
        runner.run(1, () -> lockman.release(transactions[1], dbResource));

        // Lock check
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 2L)),
                     lockman.getLocks(dbResource));

        // Block checks
        blocked_status.clear();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false), blocked_status);

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testXSXS() {
        /**
         * Transaction 0 acquires an X lock on dbResource
         * Transaction 1 attempts to acquire an S lock on dbResource but
         *    blocks due to a conflict with Transaction 0's X lock
         * Transaction 2 attempts to acquire an X lock on dbResource but
         *    blocks due to an existing queue
         * Transaction 3 attempts to acquire an S lock on dbResource but
         *    blocks due to an existing queue
         *
         * After this:
         *    Transaction 0 should have an X lock on dbResource
         *    dbResources should have [S (T1), X (T2), S (T3)] in its queue
         *    Transaction 0 should be unblocked, the rest should be blocked
         */
        DeterministicRunner runner = new DeterministicRunner(4);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.S));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.X));
        runner.run(3, () -> lockman.acquire(transactions[3], dbResource, LockType.S));

        // Lock check
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 0L)),
                     lockman.getLocks(dbResource));

        // Block checks
        List<Boolean> blocked_status = new ArrayList<>();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, true, true, true), blocked_status);

        /**
         * Transaction 0 releases its X lock on dbResource
         * Transaction 1 acquires an S lock on dbResource and unblocks
         * Transaction 2 moves forwards in the queue but remains blocked because
         *    of a conflict with Transaction 1's S lock
         * Transaction 3 moves forwards in the queue but remains blocked behind
         *    Transaction 2
         *
         * After this:
         *    Transaction 1 should have an S lock on dbResource
         *    dbResources should have [X (T2), S (T3)] in its queue
         *    Transaction 0 and 1 should be unblocked, 2 and 3 should be blocked
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // Lock check
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 1L)),
                     lockman.getLocks(dbResource));

        // Block checks
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, true, true), blocked_status);

        /**
         * Transaction 1 releases its S lock on dbResource
         * Transaction 2 acquires an X lock on dbResource and unblocks
         * Transaction 3 moves forward in the queue but remains blocked because
         *    of a conflict with Transaction 2's X lock
         *
         * After this:
         *    Transaction 2 should have an X lock on dbResource
         *    dbResources should have [S (T3)] in its queue
         *    Transaction 0, 1 and 2 should be unblocked, 3 should blocked
         */
        runner.run(1, () -> lockman.release(transactions[1], dbResource));

        // Lock check
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 2L)),
                     lockman.getLocks(dbResource));

        // Block checks
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false, true), blocked_status);

        /**
         * Transaction 2 releases its X lock on dbResource
         * Transaction 3 acquires an S lock on dbResource and unblocks
         *
         * After this:
         *    Transaction 3 should have an S lock on dbResource
         *    dbResources should have an empty queue
         *    All transactions should be unblocked
         */
        runner.run(2, () -> lockman.release(transactions[2], dbResource));

        // Lock check
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 3L)),
                     lockman.getLocks(dbResource));

        // Block checks
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false, false), blocked_status);

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLock() {
        /**
         * Transaction 0 acquires an S lock on dbResource
         * Transaction 0 promotes its S lock on dbResource to an X lock
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquire(transactions[0], dbResource, LockType.S);
            lockman.promote(transactions[0], dbResource, LockType.X);
        });

        // Lock checks
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 0L)),
                     lockman.getLocks(dbResource));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLockNotHeld() {
        DeterministicRunner runner = new DeterministicRunner(1);
        try {
            runner.run(0, () -> lockman.promote(transactions[0], dbResource, LockType.X));
            fail("Attempting to promote a lock that doesn't exist should " +
                 "throw a NoLockHeldException.");
        } catch (NoLockHeldException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLockAlreadyHeld() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        try {
            runner.run(0, () -> lockman.promote(transactions[0], dbResource, LockType.X));
            fail("Attempting to promote a lock to an equivalent lock should " +
                 "throw a DuplicateLockRequestException");
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testFIFOQueueLocks() {
        /**
         * Transaction 0 acquires an X lock on dbResource
         * Transaction 1 attempts to acquire an X lock on dbResource but
         *    blocks because of a conflict with Transaction 0's X lock
         * Transaction 2 attempts to acquire an X lock on dbResource but
         *    blocks because of an existing queue
         *
         * After this:
         *    Transaction 0 should have an X lock on dbResource
         *    dbResource should have [X (T1), X (T2)] in its queue
         */
        DeterministicRunner runner = new DeterministicRunner(3);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.X));

        // Lock checks
        assertTrue(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        /**
         * Transaction 0 releases its X lock on dbResource
         * Transaction 1 acquires an X lock on dbResource and unblocks
         * Transaction 2 moves forwards in the queue but remains blocked
         *    because of a conflict with Transaction 1's X lock
         *
         * After this:
         *    Transaction 1 should have an X lock on dbResource
         *    dbResource should have [X (T2)] in its queue
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // Lock checks
        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        /**
         * Transaction 1 releases its X lock on dbResource
         * Transaction 2 acquires an X lock on dbResource and unblocks
         *
         * After this:
         *    Transaction 2 should have an X lock on dbResource
         */
        runner.run(1, () -> lockman.release(transactions[1], dbResource));

        // Lock checks
        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[2], dbResource, LockType.X));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testStatusUpdates() {
        TransactionContext t0 = transactions[0];
        TransactionContext t1 = transactions[1];

        /**
         * Transaction 0 acquires an X lock on dbResource
         * Transaction 1 attempts to acquire an X lock on dbResource but
         *    blocks due to a conflict with Transaction 0's X lock
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(t1, dbResource, LockType.X));

        // Lock checks
        assertTrue(holds(lockman, t0, dbResource, LockType.X));
        assertFalse(holds(lockman, t1, dbResource, LockType.X));

        // Block checks
        assertFalse(t0.getBlocked());
        assertTrue(t1.getBlocked());

        /**
         * Transaction 0 releases its X lock on dbResource
         * Transaction 1 acquires an X lock on dbResource and unblocks
         */
        runner.run(0, () -> lockman.release(t0, dbResource));

        // Lock checks
        assertFalse(holds(lockman, t0, dbResource, LockType.X));
        assertTrue(holds(lockman, t1, dbResource, LockType.X));

        // Block checks
        assertFalse(t0.getBlocked());
        assertFalse(t1.getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testTableEventualUpgrade() {
        TransactionContext t0 = transactions[0];
        TransactionContext t1 = transactions[1];

        /**
         * Transaction 0 acquires an S lock on dbResource
         * Transaction 1 acquires an S lock on dbResource
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.S));
        runner.run(1, () -> lockman.acquire(t1, dbResource, LockType.S));

        assertTrue(holds(lockman, t0, dbResource, LockType.S));
        assertTrue(holds(lockman, t1, dbResource, LockType.S));

        /**
         * Transaction 0 attempts to promote its S lock to an X lock but fails
         *    due to a conflict with Transaction 1's S lock. Transaction 0
         *    queues an X lock and blocks.
         */
        runner.run(0, () -> lockman.promote(t0, dbResource, LockType.X));

        assertTrue(holds(lockman, t0, dbResource, LockType.S));
        assertFalse(holds(lockman, t0, dbResource, LockType.X));
        assertTrue(holds(lockman, t1, dbResource, LockType.S));
        assertTrue(t0.getBlocked());

        /**
         * Transaction 1 releases its S lock on dbResource
         * Transaction 0 promotes its S lock to an X lock and unblocks
         */
        runner.run(1, () -> lockman.release(t1, dbResource));

        assertTrue(holds(lockman, t0, dbResource, LockType.X));
        assertFalse(holds(lockman, t1, dbResource, LockType.S));
        assertFalse(t0.getBlocked());

        /**
         * Transaction 0 releases its X lock on dbResource
         */
        runner.run(0, () -> lockman.release(t0, dbResource));

        assertFalse(holds(lockman, t0, dbResource, LockType.X));
        assertFalse(holds(lockman, t1, dbResource, LockType.S));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testIntentBlockedAcquire() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t0 = transactions[0];
        TransactionContext t1 = transactions[1];

        /**
         * Transaction 0 acquires an S lock on dbResource
         * Transaction 1 attempts to acquire an IX lock on dbResource, but
         *    blocks due to a conflict with Transaction 0's S lock
         */
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.S));
        runner.run(1, () -> lockman.acquire(t1, dbResource, LockType.IX));
        assertFalse(t0.getBlocked());
        assertTrue(t1.getBlocked());

        // Lock checks
        assertTrue(holds(lockman, t0, dbResource, LockType.S));
        assertFalse(holds(lockman, t1, dbResource, LockType.IX));

        /**
         * Transaction 0 releases its S lock on dbResource
         * Transaction 1 acquires an IX lock on dbResource and unblocks
         */
        runner.run(0, () -> lockman.release(t0, dbResource));

        assertFalse(holds(lockman, t0, dbResource, LockType.S));
        assertTrue(holds(lockman, t1, dbResource, LockType.IX));
        assertFalse(t0.getBlocked());
        assertFalse(t1.getBlocked());

        runner.joinAll();
    }

}

