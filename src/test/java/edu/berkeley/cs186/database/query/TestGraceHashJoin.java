package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.Proj3Part1Tests;
import edu.berkeley.cs186.database.categories.Proj3Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.join.GHJOperator;
import edu.berkeley.cs186.database.query.join.SHJOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

@Category({Proj3Tests.class, Proj3Part1Tests.class})
public class TestGraceHashJoin {
    private Database d;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("ghjTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(6); // B = 6
        d.waitAllTransactions();
    }

    /**
     * Sanity Test to make sure SHJ works. You should pass this without having
     * touched GHJ.
     */
    @Test
    @Category(SystemTests.class)
    public void testSimpleSHJ() {
        try(Transaction transaction = d.beginTransaction()) {
            Schema schema = TestUtils.createSchemaWithAllTypes();

            List<Record> leftRecords = new ArrayList<>();
            List<Record> rightRecords = new ArrayList<>();
            Set<Record> expectedOutput = new HashSet<>();

            for (int i = 0; i < 10; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                leftRecords.add(r);
            }

            for (int i = 5; i < 15; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                rightRecords.add(r);
            }

            for (int i = 5; i < 10; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                expectedOutput.add(r.concat(r));
            }

            SHJOperator shj = new SHJOperator(
                new TestSourceOperator(leftRecords, schema),
                new TestSourceOperator(rightRecords, schema),
                "int", "int", transaction.getTransactionContext()
            );

            Set<Record> output = new HashSet<>();
            for (Record record : shj) output.add(record);

            assertEquals(5, output.size());
            assertEquals(expectedOutput, output);
        }
    }

    /**
     * Sanity test on a simple set of inputs. GHJ should behave similarly to SHJ
     * for this one, since they should both use the same hash function and
     * build by default on the left relation.
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleGHJ() {
        try(Transaction transaction = d.beginTransaction()) {
            List<Record> leftRecords = new ArrayList<>();
            List<Record> rightRecords = new ArrayList<>();

            List<Record> expectedOutput = new ArrayList<>();

            Schema schema = TestUtils.createSchemaWithAllTypes();

            for (int i = 0; i < 10; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                leftRecords.add(r);
            }

            for (int i = 5; i < 15; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                rightRecords.add(r);
            }

            SHJOperator shj = new SHJOperator(
                    new TestSourceOperator(leftRecords, schema),
                    new TestSourceOperator(rightRecords, schema),
                    "int", "int",
                    transaction.getTransactionContext()
            );
            for (Record expected : shj) expectedOutput.add(expected);

            GHJOperator ghj = new GHJOperator(
                    new TestSourceOperator(leftRecords, schema),
                    new TestSourceOperator(rightRecords, schema),
                    "int", "int",
                    transaction.getTransactionContext()
            );
            List<Record> output = new ArrayList<>();
            for(Record record: ghj) output.add(record);

            assertEquals(5, output.size());
            assertEquals(expectedOutput, output);
        }
    }

    /**
     * Tests GHJ with records of different schemas on some join column.
     */
    @Test
    @Category(PublicTests.class)
    public void testGHJDifferentSchemas() {
        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(3); // B=3
            Schema leftSchema = new Schema()
                .add("int", Type.intType())
                .add("string", Type.stringType(10));
            Schema rightSchema = TestUtils.createSchemaWithAllTypes();

            List<Record> leftRecords = new ArrayList<>();
            List<Record> rightRecords = new ArrayList<>();
            Set<Record> expectedOutput = new HashSet<>();

            d.setWorkMem(3);

            for (int i = 0; i < 1860; i++) {
                Record left = new Record(i, "I love 186");
                leftRecords.add(left);
            }

            for (int i = 186; i < 9300; i++) {
                Record right = TestUtils.createRecordWithAllTypesWithValue(i);
                rightRecords.add(right);
            }

            for (int i = 186; i < 1860; i++) {
                Record r1 = new Record(i, "I love 186");
                Record r2 = TestUtils.createRecordWithAllTypesWithValue(i);
                expectedOutput.add(r1.concat(r2));
            }

            GHJOperator ghj = new GHJOperator(
                    new TestSourceOperator(leftRecords, leftSchema),
                    new TestSourceOperator(rightRecords, rightSchema),
                    "int", "int",
                    transaction.getTransactionContext()
            );

            List<Record> output = new ArrayList<>();
            for (Record record: ghj) output.add(record);

            assertEquals(1674, output.size());

            for (Record r : output) {
                assertTrue("Output incorrect", expectedOutput.contains(r));
            }
        }
    }

    /**
     * Tests student's input and checks whether SHJ fails but GHJ passes.
     */
    @Test
    @Category(PublicTests.class)
    public void testBreakSHJButPassGHJ() {
        try(Transaction transaction = d.beginTransaction()) {
            Schema schema = new Schema()
                    .add("int", Type.intType())
                    .add("string", Type.stringType(500));
            Pair<List<Record>, List<Record>> inputs = GHJOperator.getBreakSHJInputs();

            List<Record> leftRecords = inputs.getFirst();;
            List<Record> rightRecords = inputs.getSecond();

            SHJOperator shj = new SHJOperator(
                    new TestSourceOperator(leftRecords, schema),
                    new TestSourceOperator(rightRecords, schema),
                    "int", "int",
                    transaction.getTransactionContext()
            );
            try {
                Iterator<Record> iter = shj.iterator();
                while(iter.hasNext()) iter.next();
                fail("SHJ worked! It shouldn't have...");
            } catch (Exception e) {
                assertEquals("Simple Hash failed for the wrong reason!",
                        "The records in this partition cannot fit in B-2 pages of memory.", e.getMessage());
            }

            GHJOperator ghj = new GHJOperator(
                    new TestSourceOperator(leftRecords, schema),
                    new TestSourceOperator(rightRecords, schema),
                    "int", "int",
                    transaction.getTransactionContext()
            );

            try {
                Iterator<Record> iter = ghj.iterator();
                while(iter.hasNext()) iter.next();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Tests student input such that GHJ breaks when using regular partitions.
     */
    @Test
    @Category(PublicTests.class)
    public void testGHJBreak() {
        try(Transaction transaction = d.beginTransaction()) {
            Schema schema = new Schema()
                    .add("int", Type.intType())
                    .add("string", Type.stringType(500));
            Pair<List<Record>, List<Record>> inputs = GHJOperator.getBreakGHJInputs();

            List<Record> leftRecords = inputs.getFirst();
            List<Record> rightRecords = inputs.getSecond();

            GHJOperator ghj = new GHJOperator(
                new TestSourceOperator(leftRecords, schema),
                new TestSourceOperator(rightRecords, schema),
                "int", "int",
                transaction.getTransactionContext()
            );

            try {
                Iterator<Record> records = ghj.iterator();
                while(records.hasNext()) records.next();
                fail("GHJ Worked! It shouldn't have...");
            } catch (Exception e) {
                assertEquals("GHJ Failed for the wrong reason...",
                        "Reached the max number of passes", e.getMessage());
            }
        }
    }

}
