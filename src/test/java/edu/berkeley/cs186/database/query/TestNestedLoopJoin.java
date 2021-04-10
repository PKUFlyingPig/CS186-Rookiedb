package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.join.BNLJOperator;
import edu.berkeley.cs186.database.query.join.PNLJOperator;
import edu.berkeley.cs186.database.query.join.SNLJOperator;
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
public class TestNestedLoopJoin {
    private Database d;
    private long numIOs;
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("nljTest");
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

    private void checkIOs(long minIOs, long maxIOs) {
        checkIOs(null, minIOs, maxIOs);
    }

    private void checkIOs(String message, long numIOs) {
        checkIOs(message, numIOs, numIOs);
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

    private void evictPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        this.d.getBufferManager().evict(pnum);
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void setSourceOperators(QueryOperator leftSourceOperator,
                                    QueryOperator rightSourceOperator) {
        assert (this.leftSourceOperator == null && this.rightSourceOperator == null);

        this.leftSourceOperator = leftSourceOperator;
        this.rightSourceOperator = rightSourceOperator;

        // hard-coded mess, but works as long as the first two tables created are the source operators
        pinPage(1, 0); // _metadata.tables header page
        pinPage(3, 0); // left source header page
        pinPage(4, 0); // right source header page
    }

    @Test
    @Category(SystemTests.class)
    public void testSimpleJoinSNLJ() {
        // Simulates joining two tables, each containing 100 identical records,
        // joined on the column "int". Since all records are identical we expect
        // expect exactly 100 x 100 = 10000 records to be yielded.
        // Both tables consist of a single page.
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithAllTypes(100),
                    TestUtils.createSourceWithAllTypes(100),
                    transaction
            );

            startCountIOs();

            // Constructing the the operator should incur no extra IOs
            JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            // Creating the iterator should incur 2 IOs, one for the first page
            // of the left relation and one for the first page of the right
            // relation.
            checkIOs(2);

            int numRecords = 0;
            Record expectedRecord = new Record(true, 1, "a", 1.2f, true, 1, "a", 1.2f);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(SystemTests.class)
    public void testNonEmptyWithEmptySNLJ() {
        // Joins a non-empty table with an empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        d.setWorkMem(4); // B=4
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithAllTypes(100),
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    transaction
            );
            startCountIOs();
            JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator,
                    "int", "int", transaction.getTransactionContext());
            checkIOs(0);
            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    @Category(SystemTests.class)
    public void testEmptyWithNonEmptySNLJ() {
        // Joins an empty table with a non-empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        d.setWorkMem(4); // B=4
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    TestUtils.createSourceWithAllTypes(100),
                    transaction
            );
            startCountIOs();
            JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator,
                    "int", "int", transaction.getTransactionContext());
            checkIOs(0);
            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    @Category(SystemTests.class)
    public void testEmptyWithEmptySNLJ() {
        // Joins a empty table with an empty table. Expected behavior is
        // that iterator is created without error, and hasNext() immediately
        // returns false.
        d.setWorkMem(4); // B=4
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    TestUtils.createSourceWithInts(Collections.emptyList()),
                    transaction
            );

            startCountIOs();

            JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator,
                    "int", "int", transaction.getTransactionContext());
            checkIOs(0);
            Iterator<Record> outputIterator = joinOperator.iterator();
            assertFalse("too many records", outputIterator.hasNext());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleJoinPNLJ() {
        // Simulates joining two tables, each containing 100 identical records,
        // joined on the column "int". Since all records are identical we expect
        // expect exactly 100 x 100 = 10,000 records to be yielded.
        // Both tables consist of a single page.
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithAllTypes(100),
                    TestUtils.createSourceWithAllTypes(100),
                    transaction
            );

            startCountIOs();

            // Constructing the the operator should incur no extra IOs
            JoinOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            // Creating the iterator should incur 2 IOs, one for the first page
            // of the left relation and one for the first page of the right
            // relation.
            checkIOs(2);

            int numRecords = 0;
            List<DataBox> expectedRecordValues = new ArrayList<>();
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            Record expectedRecord = new Record(expectedRecordValues);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }
            // Since the tables consisted of 1 page each we expect no additional
            // IOs to be incurred.
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleJoinBNLJ() {
        // This test is identical to the above test, but uses your BNLJ
        // with B=5 instead.
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                    TestUtils.createSourceWithAllTypes(100),
                    TestUtils.createSourceWithAllTypes(100),
                    transaction
            );

            startCountIOs();

            JoinOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            int numRecords = 0;
            Record expectedRecord = new Record(true, 1, "a", 1.2f, true, 1, "a", 1.2f);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePNLJOutputOrder() {
        // Constructs two tables both the schema (BOOL, INT, STRING(1), FLOAT).
        // We only use copies of two different records here:
        // Type 1: (true, 1, "1", 1.0)
        // Type 2: (true, 2, "2", 2.0)
        //
        // We join together two tables consisting of:
        // Left Table Page 1:  200 copies of Type 1, 200 copies of Type 2
        // Left Table Page 2:  200 copies of Type 2, 200 copies of Type 1
        // Right Table Page 1: 200 copies of Type 1, 200 copies of Type 2
        // Right Table Page 2: 200 copies of Type 1, 200 copies of Type 2
        // (Each page holds exactly 400 records)
        //   +-----------+
        // 1 | x   | x   |
        // 2 |   x |   x |
        // --+-----+-----+
        // 2 |   x |   x |
        // 1 | x   | x   |
        //   +-----+-----+
        //     1 2 | 1 2
        try(Transaction transaction = d.beginTransaction()) {
            // This whole section is just to generate the tables described above
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);

            Record expectedRecord1 = r1.concat(r1);
            Record expectedRecord2 = r2.concat(r2);
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

            for (int i = 0; i < 800; i++) {
                if (i < 200) {
                    transaction.insert("leftTable", r1);
                    transaction.insert("rightTable", r1);
                } else if (i < 400) {
                    transaction.insert("leftTable", r2);
                    transaction.insert("rightTable", r2);
                } else if (i < 600) {
                    transaction.insert("leftTable", r2);
                    transaction.insert("rightTable", r1);
                } else {
                    transaction.insert("leftTable", r1);
                    transaction.insert("rightTable", r2);
                }
            }

            setSourceOperators(
                new SequentialScanOperator(transaction.getTransactionContext(), "leftTable"),
                new SequentialScanOperator(transaction.getTransactionContext(), "rightTable")
            );
            pinPage(1, 1); // _metadata.tables entry for left source
            pinPage(1, 2); // _metadata.tables entry for right source

            startCountIOs();

            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            // Creating the iterator should incur 2 IOs, one for the first page
            // of the left table and one for the first page of the right table
            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            int count = 0;
            while (outputIterator.hasNext() && count < 400 * 400 * 2) {
                if (count < 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 2) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 3) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 4) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 5) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 6) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 7) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                }
                count++;

                if (count == 200 * 200 * 2 + 1) {
                    // Yielding this record should have incurred incurred 1 IO to
                    // load in the second right page.
                    checkIOs("at record " + count, 1);
                    evictPage(4, 1);
                }
                if (count == 200 * 200 * 6 + 1) {
                    // Yielding this record should have incurred incurred 1 IO to
                    // load in the second right page.
                    checkIOs("at record " + count, 1);
                    evictPage(4, 1);
                } else if (count == 200 * 200 * 4 + 1) {
                    // Yielding this record should have incurred 2 IOs, one
                    // to load in the second page of the left table, and one to
                    // load in the first page of the right table
                    checkIOs("at record " + count, 2);
                    evictPage(4, 2);
                    evictPage(3, 1);
                } else {
                    // Yielding this record should have incurred 0 IOs. If you
                    // fail the assert here you may be either skipping records
                    // or loading in a new page too early.
                    checkIOs("at record " + count, 0);
                }
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 400 * 400 * 2, count);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testBNLJDiffOutPutThanPNLJ() {
        // Constructs two tables both the schema (BOOL, INT, STRING(1), FLOAT).
        // We only use copies of four different records here:
        // Type 1: (true, 1, "1", 1.0)
        // Type 2: (true, 2, "2", 2.0)
        // Type 4: (true, 3, "3", 3.0)
        // Type 5: (true, 4, "4", 4.0)
        //
        // We join together two tables consisting of:
        // Left Table Page 1:  200 copies of Type 1, 200 copies of Type 2
        // Left Table Page 2:  200 copies of Type 3, 200 copies of Type 4
        // Right Table Page 1: 200 copies of Type 3, 200 copies of Type 4
        // Right Table Page 2: 200 copies of Type 1, 200 copies of Type 2
        // (Each page holds exactly 400 records)
        //   +-----------+
        // 4 |   x |     |
        // 3 | x   |     |
        // --+-----+-----+
        // 2 |     |   x |
        // 1 |     | x   |
        //   +-----+-----+
        //     3 4 | 1 2
        // Note that the left (vertical) relation will be processed in blocks
        // (B=4)
        d.setWorkMem(4); // B=4
        try(Transaction transaction = d.beginTransaction()) {
            // This whole section is just to generate the tables described above
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
            Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);

            Record expectedRecord1 = r1.concat(r1);
            Record expectedRecord2 = r2.concat(r2);
            Record expectedRecord3 = r3.concat(r3);
            Record expectedRecord4 = r4.concat(r4);

            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
            for (int i = 0; i < 800; i++) {
                if (i < 200) {
                    transaction.insert("leftTable", r1);
                    transaction.insert("rightTable", r3);
                } else if (i < 400) {
                    transaction.insert("leftTable", r2);
                    transaction.insert("rightTable", r4);
                } else if (i < 600) {
                    transaction.insert("leftTable", r3);
                    transaction.insert("rightTable", r1);
                } else {
                    transaction.insert("leftTable", r4);
                    transaction.insert("rightTable", r2);
                }
            }

            setSourceOperators(
                    new SequentialScanOperator(transaction.getTransactionContext(), "leftTable"),
                    new SequentialScanOperator(transaction.getTransactionContext(), "rightTable")
            );
            pinPage(1, 1); // _metadata.tables entry for left source
            pinPage(1, 2); // _metadata.tables entry for right source

            startCountIOs();

            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            // Creating the iterator should incur 3 IOs, one for the first right
            // page and two for the first left block.
            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(3);

            int count = 0;
            while (outputIterator.hasNext() && count < 4 * 200 * 200) {
                Record r = outputIterator.next();
                if (count < 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord3, r);
                } else if (count < 2 * 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord4, r);
                } else if (count < 3 * 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord1, r);
                } else {
                    assertEquals("mismatch at record " + count, expectedRecord2, r);
                }

                count++;

                if (count == 200 * 200 * 2 + 1) {
                    // Yielding this record should have incurred 1 IO to read in
                    // the second right page.
                    checkIOs("at record " + count, 1);
                } else {
                    // Yielding this record should have incurred 0 IOs. If you
                    // fail the assert here you may be either skipping records
                    // or loading in a new page too early.
                    checkIOs("at record " + count, 0);
                }
            }
            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 4 * 200 * 200, count);
        }
    }
}
