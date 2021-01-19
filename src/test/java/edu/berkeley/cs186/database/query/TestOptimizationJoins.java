package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;

import org.junit.After;

import edu.berkeley.cs186.database.TimeoutScaling;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({Proj3Tests.class, Proj3Part2Tests.class})
public class TestOptimizationJoins {
    private Database db;

    // Before every test you create a temp folder, after every test you close it
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 10 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                10000 * TimeoutScaling.factor)));

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder("testOptimizationJoins");
        String filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(5); // B=5
        this.db.waitSetupFinished();
        try(Transaction t = this.db.beginTransaction()) {
            Schema schema = TestUtils.createSchemaWithAllTypes();
            t.dropAllTables();
            t.createTable(schema, "indexed_table");
            t.createIndex("indexed_table", "int", false);
            t.createTable(schema, "table1");
            t.createTable(schema, "table2");
            t.createTable(schema, "table3");
            t.createTable(schema, "table4");
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
    public void testJoinTypeA() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table1", r);
            }

            transaction.getTransactionContext().getTable("table1").buildStatistics(10);

            // SELECT * FROM t1 AS table1
            //    INNER JOIN table1 ON t2 = t1.int;
            QueryPlan query = transaction.query("table1", "t1");
            query.join("table1", "t2", "t1.int", "t2.int");

            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();
            assertTrue(finalOperator.toString().contains("BNLJ"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinTypeB() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 10; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("indexed_table", r);
            }

            transaction.getTransactionContext().getTable("indexed_table").buildStatistics(10);

            // SELECT * FROM t1 AS indexed_table
            //    INNER JOIN indexed_table AS t2 ON t1.int = t2.int
            // WHERE t2.int = 9
            QueryPlan query = transaction.query("indexed_table", "t1");
            query.join("indexed_table", "t2", "t1.int", "t2.int");
            query.select("t1.int", PredicateOperator.EQUALS, 9);

            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();

            // Check that the select operator was pushed down
            assertTrue(finalOperator.toString().contains("\t-> Select t1.int=9"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinTypeC() {
        try(Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("indexed_table", r);
            }

            transaction.getTransactionContext().getTable("indexed_table").buildStatistics(10);

            // add a join and a select to the QueryPlan
            QueryPlan query = transaction.query("indexed_table", "t1");
            query.join("indexed_table", "t2", "t1.int", "t2.int");
            query.select("t2.int", PredicateOperator.EQUALS, 9);

            // execute the query
            query.execute();
            QueryOperator finalOperator = query.getFinalOperator();
            assertTrue(finalOperator.toString().contains("Index Scan"));
            assertTrue(finalOperator.toString().contains("SNLJ"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinOrderA() {
        try(Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 10; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table1", r);
            }

            for (int i = 0; i < 100; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table2", r);
            }

            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table3", r);
            }

            transaction.getTransactionContext().getTable("table1").buildStatistics(10);
            transaction.getTransactionContext().getTable("table2").buildStatistics(10);
            transaction.getTransactionContext().getTable("table3").buildStatistics(10);

            // SELECT * FROM table1
            //     INNER JOIN table2 ON table1.int = table2.int
            //     INNER JOIN table3 ON table2.int = table3.int
            QueryPlan query = transaction.query("table1");
            query.join("table2", "table1.int", "table2.int");
            query.join("table3", "table2.int", "table3.int");
            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();
            //inner most joins are the largest tables
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table2"));
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table3"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinOrderB() {
        try(Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 10; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table1", r);
            }

            for (int i = 0; i < 100; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table2", r);
            }

            for (int i = 0; i < 2000; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table3", r);
                transaction.insert("table4", r);
            }

            transaction.getTransactionContext().getTable("table1").buildStatistics(10);
            transaction.getTransactionContext().getTable("table2").buildStatistics(10);
            transaction.getTransactionContext().getTable("table3").buildStatistics(10);
            transaction.getTransactionContext().getTable("table4").buildStatistics(10);

            // add a join and a select to the QueryPlan
            QueryPlan query = transaction.query("table1");
            query.join("table2", "table1.int", "table2.int");
            query.join("table3", "table2.int", "table3.int");
            query.join("table4", "table1.string", "table4.string");

            // execute the query
            query.execute();
            QueryOperator finalOperator = query.getFinalOperator();

            //smallest to largest order
            assertTrue(finalOperator.toString().contains("\t\t\t-> Seq Scan on table2"));
            assertTrue(finalOperator.toString().contains("\t\t\t-> Seq Scan on table3"));
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table1"));
            assertTrue(finalOperator.toString().contains("\t-> Seq Scan on table4"));
        }
    }

}
