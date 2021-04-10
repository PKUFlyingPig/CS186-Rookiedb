package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj3Part1Tests;
import edu.berkeley.cs186.database.categories.Proj3Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.*;
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
public class TestSortOperator {
    private Database d;
    private Page metadataHeader;
    private Page indexHeader;
    private long numIOs;

    // 1 extra I/O on first access to a table after evictAll
    public static long FIRST_ACCESS_IOS = 1;
    // 1 I/O to create a header page
    public static long NEW_RUN_IOS = 1;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 2 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            5000 * TimeoutScaling.factor)));

    @Ignore
    public static class SortRecordComparator implements Comparator<Record> {
        private int columnIndex;

        private SortRecordComparator(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public int compare(Record o1, Record o2) {
            return o1.getValue(this.columnIndex).compareTo(
                    o2.getValue(this.columnIndex));
        }
    }

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("sortTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.waitAllTransactions();
        long pageNum = DiskSpaceManager.getVirtualPageNum(1, 0);
        metadataHeader = d.getBufferManager().fetchPage(new DummyLockContext(), pageNum);
        pageNum = DiskSpaceManager.getVirtualPageNum(2, 0);
        indexHeader = d.getBufferManager().fetchPage(new DummyLockContext(), pageNum);
    }

    @After
    public void cleanup() {
        d.waitAllTransactions();
        metadataHeader.unpin();
        indexHeader.unpin();
        d.close();
    }

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

    private void checkIOs(long minIOs, long maxIOs) {
        checkIOs(null, minIOs, maxIOs);
    }

    @Test
    @Category(PublicTests.class)
    public void testSortRun() {
        try (Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(3); // B=3, 8 records per page
            Schema schema = new Schema().add("string", Type.stringType(400));

            // Constructing a SortOperator should incur 0 I/Os
            startCountIOs();
            SortOperator operator = new SortOperator(
                    transaction.getTransactionContext(),
                    new TestSourceOperator(schema),
                    "string"
            );
            checkIOs(0);

            // Create 3 pages of records and randomly shuffle them
            List<Record> records = new ArrayList<>();
            for (int i = 0; i < 8 * 3; i++) {
                records.add(new Record(String.format("%02d", i)));
            }
            Collections.shuffle(records, new Random(42));

            // sortRun should create a new sorted run with 3 pages of records
            startCountIOs();
            Run sortedRun = operator.sortRun(records.iterator());
            checkIOs(3 + NEW_RUN_IOS);

            // Check for correct number of records and correct order
            Iterator<Record> iterator = sortedRun.iterator();
            int count = 0;
            while (iterator.hasNext() && count < 8 * 3) {
                Record expected = new Record(String.format("%02d", count));
                Record actual = iterator.next();
                assertEquals("mismatch at record " + count, expected, actual);
                count++;
            }
            assertFalse("too many records", iterator.hasNext());
            assertEquals("too few records", 8  * 3, count);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testMergeSortedRuns() {
        try (Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(3); // B=3, 8 records per page
            Schema schema = new Schema().add("string", Type.stringType(400));

            // Constructing a SortOperator should incur 0 I/Os
            startCountIOs();
            SortOperator operator = new SortOperator(
                    transaction.getTransactionContext(),
                    new TestSourceOperator(schema),
                    "string"
            );
            checkIOs(0);

            // Create 2 runs with 1.5 pages of records each
            Run run1 = operator.makeRun();
            Run run2 = operator.makeRun();
            List<Run> runs = Arrays.asList(run1, run2);
            for (int i = 0; i < 8 * 3; i++) {
                Record record = new Record(String.format("%02d", i));
                if (i % 2 == 0) run1.add(record); // even records go in run1
                else run2.add(record); // odd records go in run2
            }
            checkIOs(2 * (NEW_RUN_IOS + 2));

            // Merge the two runs
            startCountIOs();
            Run mergedSortedRuns = operator.mergeSortedRuns(runs);
            // Access 2 runs with 1.5 pages of records each
            // Create 1 run with 3 pages of records
            checkIOs(2 * (2 + FIRST_ACCESS_IOS) + (3 + NEW_RUN_IOS));

            // Check for correct number of records and correct order
            Iterator<Record> iterator = mergedSortedRuns.iterator();
            int count = 0;
            while (iterator.hasNext() && count < 8 * 3) {
                Record expected = new Record(String.format("%02d", count));
                Record actual = iterator.next();
                assertEquals("mismatch at record " + count, expected, actual);
                count++;
            }
            assertFalse("too many records", iterator.hasNext());
            assertEquals("too few records", 8 * 3, count);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testMergePass() {
        try (Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(3); // B=3
            List<Record> records1 = new ArrayList<>();
            List<Record> records2 = new ArrayList<>();
            startCountIOs();

            SortOperator s = new SortOperator(
                    transaction.getTransactionContext(),
                    TestUtils.createSourceWithAllTypes(0),
                    "int"
            );
            checkIOs(0);

            Run r1 = s.makeRun();
            Run r2 = s.makeRun();
            Run r3 = s.makeRun();
            Run r4 = s.makeRun();

            for (int i = 0; i < 400 * 4; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                if (i % 4 == 0) {
                    r1.add(r);
                    records2.add(r);
                } else if (i % 4 == 1) {
                    r2.add(r);
                    records1.add(r);
                } else if (i % 4 == 2) {
                    r3.add(r);
                    records1.add(r);
                } else {
                    r4.add(r);
                    records2.add(r);
                }
            }

            // Create 4 runs with 1 page of records each
            checkIOs(4 * (NEW_RUN_IOS + 1));

            List<Run> runs = new ArrayList<>();
            runs.add(r3);
            runs.add(r2);
            runs.add(r1);
            runs.add(r4);

            startCountIOs();
            List<Run> result = s.mergePass(runs);
            assertEquals("wrong number of runs", 2, result.size());

            // Access 4 runs with 1 page of records each
            // Create 2 runs with 2 pages of records each
            checkIOs(4 * (1 + FIRST_ACCESS_IOS) + 2 * (2 + NEW_RUN_IOS));

            Iterator<Record> iter1 = result.get(0).iterator();
            Iterator<Record> iter2 = result.get(1).iterator();
            int i = 0;
            while (iter1.hasNext() && i < 400 * 2) {
                assertEquals("mismatch at record " + i, records1.get(i), iter1.next());
                i++;
            }
            assertFalse("too many records", iter1.hasNext());
            assertEquals("too few records", 400 * 2, i);
            i = 0;
            while (iter2.hasNext() && i < 400 * 2) {
                assertEquals("mismatch at record " + i, records2.get(i), iter2.next());
                i++;
            }
            assertFalse("too many records", iter2.hasNext());
            assertEquals("too few records", 400 * 2, i);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortNoChange() {
        try (Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(3); // B=3
            List<Record> records = new ArrayList<>(400 * 3);
            for (int i = 0; i < 400 * 3; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                records.add(r);
            }

            startCountIOs();
            SortOperator s = new SortOperator(
                    transaction.getTransactionContext(),
                    new TestSourceOperator(records, TestUtils.createSchemaWithAllTypes()),
                    "int"
            );
            checkIOs(0);

            // Create 1 run with 3 pages of records
            Run sortedRun = s.sort();
            checkIOs(3 + NEW_RUN_IOS);

            Iterator<Record> iter = sortedRun.iterator();
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                assertEquals("mismatch at record " + i, records.get(i), iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortBackwards() {
        try (Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(3); // B=3
            List<Record> records = new ArrayList<>(400 * 3);
            for (int i = 400 * 3; i > 0; i--) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                records.add(r);
            }

            startCountIOs();

            SortOperator s = new SortOperator(
                    transaction.getTransactionContext(),
                    new TestSourceOperator(records, TestUtils.createSchemaWithAllTypes()),
                    "int"
            );
            checkIOs(0);

            // Create 1 run with 3 pages of records
            Run sortedRun = s.sort();
            checkIOs(3 + NEW_RUN_IOS);

            Iterator<Record> iter = sortedRun.iterator();
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                Record expected = TestUtils.createRecordWithAllTypesWithValue(i + 1);
                assertEquals("mismatch at record " + i, expected, iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortRandomOrder() {
        try (Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(3); // B=3
            List<Record> recordsToShuffle = new ArrayList<>();
            for (int i = 0; i < 400 * 3; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                recordsToShuffle.add(r);
            }
            Collections.shuffle(recordsToShuffle, new Random(42));

            startCountIOs();

            SortOperator s = new SortOperator(
                    transaction.getTransactionContext(),
                    new TestSourceOperator(recordsToShuffle, TestUtils.createSchemaWithAllTypes()),
                    "int"
            );
            checkIOs(0);

            // Create 1 run with 3 pages of records
            Run sortedRun = s.sort();
            checkIOs(3 + NEW_RUN_IOS);

            Iterator<Record> iter = sortedRun.iterator();
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                Record expected = TestUtils.createRecordWithAllTypesWithValue(i);
                assertEquals("mismatch at record " + i, expected, iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
            checkIOs(0);
        }
    }

}