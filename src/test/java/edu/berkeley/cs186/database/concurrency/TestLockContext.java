package edu.berkeley.cs186.database.concurrency;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

@Category({Proj4Tests.class, Proj4Part2Tests.class})
public class TestLockContext {
    private LoggingLockManager lockManager;

    private LockContext dbLockContext;
    private LockContext tableLockContext;
    private LockContext pageLockContext;

    private TransactionContext[] transactions;

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Before
    public void setUp() {
        lockManager = new LoggingLockManager();
        /**
         * For all of these tests we have the following resource hierarchy
         *                     database
         *                        |
         *                      table1
         *                        |
         *                      page1
         */
        dbLockContext = lockManager.databaseContext();
        tableLockContext = dbLockContext.childContext("table1");
        pageLockContext = tableLockContext.childContext("page1");

        transactions = new TransactionContext[8];
        for (int i = 0; i < transactions.length; i++) {
            transactions[i] = new DummyTransactionContext(lockManager, i);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireFail() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        try {
            tableLockContext.acquire(transactions[0], LockType.X);
            fail("Attempting to acquire an X lock with an IS lock on " +
                 "the parent should throw an InvalidLockException.");
        } catch (InvalidLockException e) {
            // do nothing
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquirePass() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        tableLockContext.acquire(transactions[0], LockType.S);
        // both locks should have been acquired
        Assert.assertEquals(Arrays.asList(new Lock(dbLockContext.getResourceName(), LockType.IS, 0L),
                                          new Lock(tableLockContext.getResourceName(), LockType.S, 0L)),
                            lockManager.getLocks(transactions[0]));
    }

    @Test
    @Category(PublicTests.class)
    public void testTreeAcquirePass() {
        dbLockContext.acquire(transactions[0], LockType.IX);
        tableLockContext.acquire(transactions[0], LockType.IS);
        pageLockContext.acquire(transactions[0], LockType.S);
        // all three locks should have been acquired
        Assert.assertEquals(Arrays.asList(new Lock(dbLockContext.getResourceName(), LockType.IX, 0L),
                                          new Lock(tableLockContext.getResourceName(), LockType.IS, 0L),
                                          new Lock(pageLockContext.getResourceName(), LockType.S, 0L)),
                            lockManager.getLocks(transactions[0]));
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleReleasePass() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        tableLockContext.acquire(transactions[0], LockType.S);
        tableLockContext.release(transactions[0]);
        // After the sequence above, T0 should only have its lock on the database
        Assert.assertEquals(Collections.singletonList(new Lock(dbLockContext.getResourceName(), LockType.IS,
                            0L)),
                            lockManager.getLocks(transactions[0]));
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleReleaseFail() {
        dbLockContext.acquire(transactions[0], LockType.IS);
        tableLockContext.acquire(transactions[0], LockType.S);
        try {
            dbLockContext.release(transactions[0]);
            fail("Attemptng to release an IS lock when a child resource " +
                 "still holds an S locks should throw an InvalidLockException");
        } catch (InvalidLockException e) {
            // do nothing
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSharedPage() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t1 = transactions[1];
        TransactionContext t2 = transactions[2];

        LockContext r0 = tableLockContext;
        LockContext r1 = pageLockContext;

        runner.run(0, () -> dbLockContext.acquire(t1, LockType.IS));
        runner.run(1, () -> dbLockContext.acquire(t2, LockType.IS));
        runner.run(0, () -> r0.acquire(t1, LockType.IS));
        runner.run(1, () -> r0.acquire(t2, LockType.IS));
        runner.run(0, () -> r1.acquire(t1, LockType.S));
        runner.run(1, () -> r1.acquire(t2, LockType.S));

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertTrue(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.S));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSandIS() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t1 = transactions[1];
        TransactionContext t2 = transactions[2];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        runner.run(0, () -> r0.acquire(t1, LockType.S));
        runner.run(1, () -> r0.acquire(t2, LockType.IS));
        runner.run(1, () -> r1.acquire(t2, LockType.S));
        runner.run(0, () -> r0.release(t1));

        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.S));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSharedIntentConflict() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t1 = transactions[1];
        TransactionContext t2 = transactions[2];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        runner.run(0, () -> r0.acquire(t1, LockType.IS));
        runner.run(1, () -> r0.acquire(t2, LockType.IX));
        runner.run(0, () -> r1.acquire(t1, LockType.S));
        runner.run(1, () -> r1.acquire(t2, LockType.X));

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IX));
        assertTrue(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.X));

        runner.join(0);
    }

    @Test
    @Category(PublicTests.class)
    public void testSharedIntentConflictRelease() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t1 = transactions[1];
        TransactionContext t2 = transactions[2];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        runner.run(0, () -> r0.acquire(t1, LockType.IS));
        runner.run(1, () -> r0.acquire(t2, LockType.IX));
        runner.run(0, () -> r1.acquire(t1, LockType.S));
        runner.run(1, () -> r1.acquire(t2, LockType.X));
        runner.run(0, () -> r1.release(t1));

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.IS));
        assertTrue(TestLockManager.holds(lockManager, t2, r0.getResourceName(), LockType.IX));
        assertFalse(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertTrue(TestLockManager.holds(lockManager, t2, r1.getResourceName(), LockType.X));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromote() {
        TransactionContext t1 = transactions[1];
        dbLockContext.acquire(t1, LockType.S);
        dbLockContext.promote(t1, LockType.X);
        assertTrue(TestLockManager.holds(lockManager, t1, dbLockContext.getResourceName(), LockType.X));
    }

    @Test
    @Category(PublicTests.class)
    public void testEscalateFail() {
        TransactionContext t1 = transactions[1];

        LockContext r0 = dbLockContext;

        try {
            r0.escalate(t1);
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testEscalateISS() {
        TransactionContext t1 = transactions[1];

        LockContext r0 = dbLockContext;

        r0.acquire(t1, LockType.IS);
        r0.escalate(t1);
        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.S));
    }

    @Test
    @Category(PublicTests.class)
    public void testEscalateIXX() {
        TransactionContext t1 = transactions[1];

        LockContext r0 = dbLockContext;

        r0.acquire(t1, LockType.IX);
        r0.escalate(t1);
        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.X));
    }

    @Test
    @Category(PublicTests.class)
    public void testEscalateIdempotent() {
        TransactionContext t1 = transactions[1];

        LockContext r0 = dbLockContext;

        r0.acquire(t1, LockType.IS);
        r0.escalate(t1);
        lockManager.startLog();
        r0.escalate(t1);
        r0.escalate(t1);
        r0.escalate(t1);
        assertEquals(Collections.emptyList(), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testEscalateS() {
        TransactionContext t1 = transactions[1];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;

        r0.acquire(t1, LockType.IS);
        r1.acquire(t1, LockType.S);
        r0.escalate(t1);

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
    }

    @Test
    @Category(PublicTests.class)
    public void testEscalateMultipleS() {
        TransactionContext t1 = transactions[1];

        LockContext r0 = dbLockContext;
        LockContext r1 = tableLockContext;
        LockContext r2 = dbLockContext.childContext("table2");
        LockContext r3 = dbLockContext.childContext("table3");

        r0.acquire(t1, LockType.IS);
        r1.acquire(t1, LockType.S);
        r2.acquire(t1, LockType.IS);
        r3.acquire(t1, LockType.S);

        assertEquals(3, r0.getNumChildren(t1));
        r0.escalate(t1);
        assertEquals(0, r0.getNumChildren(t1));

        assertTrue(TestLockManager.holds(lockManager, t1, r0.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r1.getResourceName(), LockType.S));
        assertFalse(TestLockManager.holds(lockManager, t1, r2.getResourceName(), LockType.IS));
        assertFalse(TestLockManager.holds(lockManager, t1, r3.getResourceName(), LockType.S));
    }

    @Test
    @Category(PublicTests.class)
    public void testGetLockType() {
        DeterministicRunner runner = new DeterministicRunner(4);

        TransactionContext t1 = transactions[1];
        TransactionContext t2 = transactions[2];
        TransactionContext t3 = transactions[3];
        TransactionContext t4 = transactions[4];

        runner.run(0, () -> dbLockContext.acquire(t1, LockType.S));
        runner.run(1, () -> dbLockContext.acquire(t2, LockType.IS));
        runner.run(2, () -> dbLockContext.acquire(t3, LockType.IS));
        runner.run(3, () -> dbLockContext.acquire(t4, LockType.IS));

        runner.run(1, () -> tableLockContext.acquire(t2, LockType.S));
        runner.run(2, () -> tableLockContext.acquire(t3, LockType.IS));

        runner.run(2, () -> pageLockContext.acquire(t3, LockType.S));

        assertEquals(LockType.S, pageLockContext.getEffectiveLockType(t1));
        assertEquals(LockType.S, pageLockContext.getEffectiveLockType(t2));
        assertEquals(LockType.S, pageLockContext.getEffectiveLockType(t3));
        assertEquals(LockType.NL, pageLockContext.getEffectiveLockType(t4));
        assertEquals(LockType.NL, pageLockContext.getExplicitLockType(t1));
        assertEquals(LockType.NL, pageLockContext.getExplicitLockType(t2));
        assertEquals(LockType.S, pageLockContext.getExplicitLockType(t3));
        assertEquals(LockType.NL, pageLockContext.getExplicitLockType(t4));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testReadonly() {
        dbLockContext.disableChildLocks();
        LockContext tableContext = dbLockContext.childContext("table2");
        TransactionContext t1 = transactions[1];
        dbLockContext.acquire(t1, LockType.IX);
        try {
            tableContext.acquire(t1, LockType.IX);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
        try {
            tableContext.release(t1);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
        try {
            tableContext.promote(t1, LockType.IX);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
        try {
            tableContext.escalate(t1);
            fail();
        } catch (UnsupportedOperationException e) {
            // do nothing
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testGetNumChildren() {
        LockContext tableContext = dbLockContext.childContext("table2");
        TransactionContext t1 = transactions[1];
        dbLockContext.acquire(t1, LockType.IX);
        tableContext.acquire(t1, LockType.IS);
        assertEquals(1, dbLockContext.getNumChildren(t1));
        tableContext.promote(t1, LockType.IX);
        assertEquals(1, dbLockContext.getNumChildren(t1));
        tableContext.release(t1);
        assertEquals(0, dbLockContext.getNumChildren(t1));
        tableContext.acquire(t1, LockType.IS);
        dbLockContext.escalate(t1);
        assertEquals(0, dbLockContext.getNumChildren(t1));
    }

}
