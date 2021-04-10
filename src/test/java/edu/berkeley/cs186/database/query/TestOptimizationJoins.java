package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj3Part2Tests;
import edu.berkeley.cs186.database.categories.Proj3Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.query.join.BNLJOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
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
import java.util.*;

import static org.junit.Assert.*;

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
    public void testMinCostJoins() {
        try (Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 500; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table1", r);
            }

            for (int i = 0; i < 1500; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table2", r);
            }

            for (int i = 0; i < 2700; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table3", r);
            }

            transaction.getTransactionContext().getTable("table1").buildStatistics(10);
            transaction.getTransactionContext().getTable("table2").buildStatistics(10);
            transaction.getTransactionContext().getTable("table3").buildStatistics(10);

            // SELECT * FROM table1
            //     INNER JOIN table2 ON table1.int = table2.int
            //     INNER JOIN table3 ON table2.int = table3.int
            // table1 is 2 pages
            // table2 is 4 pages
            // table3 is 7 pages
            QueryPlan query = transaction.query("table1");
            query.join("table2", "table1.int", "table2.int");
            query.join("table3", "table2.int", "table3.int");

            // Create Pass 1 Map for the three tables
            String[] tables = {"table1", "table2", "table3"};
            Map<Set<String>, QueryOperator> pass1Map = new HashMap<>();
            for (int i = 0; i < tables.length; i++) {
                String name = tables[i];
                pass1Map.put(Collections.singleton(name), query.minCostSingleAccess(name));
            }

            // Run one pass. After this pass the following joins should be considered:
            // - {table1, table2}
            // - {table2, table3}
            Map<Set<String>, QueryOperator> pass2Map = query.minCostJoins(pass1Map, pass1Map);
            assertEquals(2, pass2Map.size());
            Set<String> set12 = new HashSet<>();
            set12.add("table1");
            set12.add("table2");
            assertTrue(pass2Map.containsKey(set12));

            Set<String> set23 = new HashSet<>();
            set23.add("table2");
            set23.add("table3");
            assertTrue(pass2Map.containsKey(set23));

            // The following joins should have been considered for 1 and 2
            // (table2 BNLJ table1), cost=8
            // (table1 BNLJ table2), cost=6
            QueryOperator op12 = pass2Map.get(set12);
            assertTrue(op12 instanceof BNLJOperator);
            assertEquals(6, op12.estimateIOCost());

            // The following joins should have been considered for 2 and 3
            // (table3 BNLJ table2), cost=19
            // (table2 BNLJ table3), cost=18
            QueryOperator op23 = pass2Map.get(set23);
            assertTrue(op23 instanceof  BNLJOperator);
            assertEquals(18, op23.estimateIOCost());

            // Runs another pass
            Map<Set<String>, QueryOperator> pass3Map = query.minCostJoins(pass2Map, pass1Map);
            assertEquals(1, pass3Map.size());
            Set<String> set123 = new HashSet<>();
            set123.add("table2");
            set123.add("table3");
            set123.add("table1");
            assertTrue(pass3Map.containsKey(set123));

            // The following joins should have been considered:
            // - ((table1 BNLJ table2) BNLJ table3), cost=13
            // - ((table2 BNLJ table3) BNLJ table1), cost=24

            QueryOperator op123 = pass3Map.get(set123);
            assertTrue(op123 instanceof BNLJOperator);
            assertEquals(13, op123.estimateIOCost());
        }
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
            assertTrue(finalOperator.toString().contains("SNLJ") || finalOperator.toString().contains("BNLJ"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinOrderA() {
        try(Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 500; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table1", r);
            }

            for (int i = 0; i < 1500; ++i) {
                Record r = new Record(false, i, "!", 0.0f);
                transaction.insert("table2", r);
            }

            for (int i = 0; i < 2700; ++i) {
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
            //inner most joins are the smaller tables
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table1"));
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table2"));
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

            // The smaller 3 tables should be joined together first
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table2"));
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table3"));
            assertTrue(finalOperator.toString().contains("\t\t-> Seq Scan on table1"));

            // Largest table should be last to be joined in
            assertTrue(finalOperator.toString().contains("\t-> Seq Scan on table4"));
        }
    }

}
