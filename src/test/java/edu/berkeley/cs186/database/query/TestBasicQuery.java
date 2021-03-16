package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;

import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;

import static org.junit.Assert.*;
import org.junit.After;

import edu.berkeley.cs186.database.TimeoutScaling;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

@Category({Proj3Tests.class, Proj3Part2Tests.class})
public class TestBasicQuery {
    private Database db;

    // Before every test you create a temp folder, after every test you close it
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 1 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder("basicQueryTest");
        String filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(5); // B=5

        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
            Schema schema = TestUtils.createSchemaWithAllTypes();
            t.createTable(schema, "table");
        }
        this.db.waitAllTransactions();
    }

    @After
    public void afterEach() {
        this.db.waitAllTransactions();
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
    }

    @Test
    @Category(PublicTests.class)
    public void testProject() {
        try(Transaction transaction = this.db.beginTransaction()) {
            // creates a 10 records int 0 to 9
            for (int i = 0; i < 10; ++i) {
                transaction.insert("table", new Record(false, i, "!", 0.0f));
            }
            transaction.getTransactionContext().getTable("table").buildStatistics(10);

            // SELECT int FROM table;
            QueryPlan query = transaction.query("table");
            query.project("int");
            Iterator<Record> queryOutput = query.execute();

            // each each output record only have the `int` column
            int count = 0;
            while (queryOutput.hasNext()) {
                Record r = queryOutput.next();
                assertEquals(r.size(), 1);
                assertEquals(new IntDataBox(count), r.getValue(0));
                count++;
            }
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSelect() {
        try(Transaction transaction = db.beginTransaction()) {
            // creates 10 records with column `int` ranging from 0 to 9
            for (int i = 0; i < 10; ++i) {
                transaction.insert("table", new Record(false, i, "!", 0.0f));
            }
            transaction.getTransactionContext().getTable("table").buildStatistics(10);

            // SELECT * FROM table WHERE int = 9;
            QueryPlan query = transaction.query("table");
            query.select("int", PredicateOperator.EQUALS, 9);
            Iterator<Record> queryOutput = query.execute();

            // there should be exactly one record after the selection `int = 9`
            assertTrue(queryOutput.hasNext());
            Record r = queryOutput.next();
            assertEquals(new IntDataBox(9), r.getValue(1));
            assertFalse(queryOutput.hasNext());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testGroupBy() {
        try(Transaction transaction = db.beginTransaction()) {
            // creates 100 records with column `int` ranging from 0 to 9
            for (int i = 0; i < 100; ++i) {
                transaction.insert("table", new Record(false, i % 10, "!", 0.0f));
            }
            transaction.getTransactionContext().getTable("table").buildStatistics(10);

            // SELECT COUNT(*) FROM table GROUP BY int;
            QueryPlan query = transaction.query("table");
            query.groupBy("int");
            query.project("COUNT(*)");
            Iterator<Record> queryOutput = query.execute();

            // tests to see if projects/group by are applied properly
            int count = 0;
            while (queryOutput.hasNext()) {
                Record r = queryOutput.next();
                assertEquals(r.getValue(0), new IntDataBox(10));
                count++;
            }
            assertEquals(count, 10);
        }
    }

}
