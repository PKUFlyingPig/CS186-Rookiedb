package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({Proj4Tests.class, Proj4Part2Tests.class})
public class TestLockUtil {
    private LoggingLockManager lockManager;
    private TransactionContext transaction;
    private LockContext dbContext;
    private LockContext tableContext;
    private LockContext[] pageContexts;

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Before
    public void setUp() {
        /**
         * For all of these tests we have the following resource hierarchy
         *                     database
         *                        |
         *                      table1
         *         _____________/ / \ \_______________
         *       /     /     /   /   \    \     \     \
         *  page0 page1 page2 page3 page4 page5 page6 page7
         */
        lockManager = new LoggingLockManager();
        transaction = new DummyTransactionContext(lockManager, 0);
        dbContext = lockManager.databaseContext();
        tableContext = dbContext.childContext("table1");
        pageContexts = new LockContext[8];
        for (int i = 0; i < pageContexts.length; ++i) {
            pageContexts[i] = tableContext.childContext((long) i);
        }
        TransactionContext.setTransaction(transaction);
    }

    @Test
    @Category(SystemTests.class)
    public void testRequestNullTransaction() {
        /**
         * Calls to ensureSufficientLockHeld should do nothing if there isn't
         * a current transaction.
         */
        lockManager.startLog();
        TransactionContext.setTransaction(null);
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Collections.emptyList(), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquire() {
        /**
         * Requesting S on page 4 should get correct locks on ancestors
         * (IS on database, IS on table1) and grant S on page 4.
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                         "acquire 0 database IS",
                         "acquire 0 database/table1 IS",
                         "acquire 0 database/table1/4 S"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromote() {
        /**
         * Requesting S on page 4 should get IS on database, IS on table1 and
         * grant S on page 4.
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/4 S"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * Afterwards, requesting X on page 4 should promote the IS locks on
         * database and table1 to IX, and promote the S lock on page 4 to X.
         */
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.X);
        assertEquals(Arrays.asList(
                "promote 0 database IX",
                "promote 0 database/table1 IX",
                "promote 0 database/table1/4 X"
         ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testIStoS() {
        /**
         * We start by requesting S on page 4 like the previous two tests, and
         * then release the lock on page 4.
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        pageContexts[4].release(transaction);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/4 S",
                "release 0 database/table1/4"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * Afterwards we should have IS(database) and IS(table1).
         * When we request S on table1, we should release the IS lock on table1
         * and acquire an S lock in its place using acquire-and-release.
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 S [database/table1]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleEscalate() {
        /**
         * We start by requesting S on page 4 like the previous three tests
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/4 S"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * Escalating on the table should release IS(table1) and S(page 4) and
         * acquire S(table1).
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 S [database/table1, database/table1/4]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testIXBeforeIS() {
        /**
         * We start by requesting X on page 3, which should cause you to get
         * IX on database and IX on table1 as well.
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);;
        assertEquals(Arrays.asList(
                "acquire 0 database IX",
                "acquire 0 database/table1 IX",
                "acquire 0 database/table1/3 X"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * Afterwards, requesting S on page 4 should be possible using the
         * existing IX lock on table1 without any other acquisitions.
         *
         *          IX(database)
         *                |
         *           IX(table1)
         *             /     \
         *        X(page3) S(page4)
         */
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire 0 database/table1/4 S"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSIX1() {
        /**
         * We start by requesting X on page 3, which should cause you to get
         * IX on database and IX on table1 as well.
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);
        assertEquals(Arrays.asList(
                "acquire 0 database IX",
                "acquire 0 database/table1 IX",
                "acquire 0 database/table1/3 X"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * Afterwards, requesting S on table1 should promote table1's IX lock
         * to SIX.
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 SIX [database/table1]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSIX2() {
        /**
         * We request S on page1, S on page2, and X on page 3. This should give
         * us the following structure:
         *
         *          IX(database)
         *                |
         *           IX(table1)
         *          /     |    \
         *   S(page1) S(page2) X(page3)
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[1], LockType.S);
        LockUtil.ensureSufficientLockHeld(pageContexts[2], LockType.S);
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/1 S",
                "acquire 0 database/table1/2 S",
                "promote 0 database IX",
                "promote 0 database/table1 IX",
                "acquire 0 database/table1/3 X"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * Afterwards we request S on table1. Since table1 currently holds IX,
         * this should promote it to SIX. Remember that promotions to SIX should
         * release any IS/S descendants, in this case the locks on pages 1 and 2
         *
         *          IX(database)
         *                |
         *           SIX(table1)
         *          /     |    \
         *   NL(page1) NL(page2) X(page3)
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 SIX [database/table1, database/table1/1, database/table1/2]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleNL() {
        /**
         * Requesting NL should do nothing.
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.NL);
        assertEquals(Collections.emptyList(), lockManager.log);
    }

}

