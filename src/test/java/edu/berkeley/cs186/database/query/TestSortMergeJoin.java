package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj3Part1Tests;
import edu.berkeley.cs186.database.categories.Proj3Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.join.SortMergeOperator;
import edu.berkeley.cs186.database.table.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

@Category({Proj3Tests.class, Proj3Part1Tests.class})
public class TestSortMergeJoin {
    private Database d;
    private long numIOs;
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("smjTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(5); // B=5
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        for (Page p : pinnedPages.values()) p.unpin();
        d.close();
    }

    // 4 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            4000 * TimeoutScaling.factor)));

    private void startCountIOs() {
        d.getBufferManager().evictAll();
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void checkIOs(String message, long minIOs, long maxIOs) {
        if (message == null) message = "";
        else message = "(" + message + ")";
        long newIOs = d.getBufferManager().getNumIOs();
        long IOs = newIOs - numIOs;
        assertTrue(IOs + " I/Os not between " + minIOs + " and " + maxIOs + message,
                minIOs <= IOs && IOs <= maxIOs);
        numIOs = newIOs;
    }

    private void checkIOs(long numIOs) {
        checkIOs(null, numIOs, numIOs);
    }

    private void setSourceOperators(TestSourceOperator leftSourceOperator,
                                    TestSourceOperator rightSourceOperator, Transaction transaction) {
        setSourceOperators(
                new MaterializeOperator(leftSourceOperator, transaction.getTransactionContext()),
                new MaterializeOperator(rightSourceOperator, transaction.getTransactionContext())
        );
    }

    private void pinPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        Page page = d.getBufferManager().fetchPage(new DummyLockContext(), pnum);
        this.pinnedPages.put(pnum, page);
    }

    private void setSourceOperators(QueryOperator leftSourceOperator,
                                    QueryOperator rightSourceOperator) {
        assert (this.leftSourceOperator == null && this.rightSourceOperator == null);

        this.leftSourceOperator = leftSourceOperator;
        this.rightSourceOperator = rightSourceOperator;

        // hard-coded mess, but works as long as the first two tables created are the source operators
        pinPage(1, 0); // _metadata.tables header page
        pinPage(2, 0); // _metadata.indices header page
        pinPage(3, 0); // left source header page
        pinPage(4, 0); // right source header page
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleSortMergeJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithAllTypes(100),
                    TestUtils.createSourceWithAllTypes(100),
                    transaction
            );

            startCountIOs();

            JoinOperator joinOperator = new SortMergeOperator(
                    leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2 * (1 + (1 + TestSortOperator.NEW_RUN_IOS)));

            int numRecords = 0;
            Record expected = new Record(true, 1, "a", 1.2f, true, 1, "a", 1.2f);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expected, outputIterator.next());
                numRecords++;
            }
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            outputIterator.hasNext();
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortMergeJoinUnsortedInputs()  {
        d.setWorkMem(3); // B=3
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
            pinPage(1, 1);
            pinPage(1, 2);

            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
            Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);

            Record expectedRecord1 = r1.concat(r1);
            Record expectedRecord2 = r2.concat(r2);
            Record expectedRecord3 = r3.concat(r3);
            Record expectedRecord4 = r4.concat(r4);

            List<Record> leftTableRecords = new ArrayList<>();
            List<Record> rightTableRecords = new ArrayList<>();
            for (int i = 0; i < 800; i++) {
                Record r;
                if (i % 4 == 0) r = r1;
                else if (i % 4 == 1) r = r2;
                else if (i % 4 == 2) r = r3;
                else r = r4;
                leftTableRecords.add(r);
                rightTableRecords.add(r);
            }
            Collections.shuffle(leftTableRecords, new Random(10));
            Collections.shuffle(rightTableRecords, new Random(20));
            for (int i = 0; i < 800; i++) {
                transaction.insert("leftTable", leftTableRecords.get(i));
                transaction.insert("rightTable", rightTableRecords.get(i));
            }

            setSourceOperators(
                    new SequentialScanOperator(transaction.getTransactionContext(), "leftTable"),
                    new SequentialScanOperator(transaction.getTransactionContext(), "rightTable")
            );

            startCountIOs();

            JoinOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "int",
                    "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2 * (2 + (2 + TestSortOperator.NEW_RUN_IOS)));

            int numRecords = 0;
            Record expectedRecord;

            while (outputIterator.hasNext() && numRecords < 400 * 400) {
                if (numRecords < (400 * 400 / 4)) {
                    expectedRecord = expectedRecord1;
                } else if (numRecords < (400 * 400 / 2)) {
                    expectedRecord = expectedRecord2;
                } else if (numRecords < 400 * 400 - (400 * 400 / 4)) {
                    expectedRecord = expectedRecord3;
                } else {
                    expectedRecord = expectedRecord4;
                }
                Record r = outputIterator.next();
                assertEquals("mismatch at record " + numRecords, expectedRecord, r);
                numRecords++;
            }
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 400 * 400, numRecords);
        }
    }

}
