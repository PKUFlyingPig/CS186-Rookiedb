package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category({Proj99Tests.class, SystemTests.class})
public class TestDatabase {
    private static final String TestDir = "testDatabase";
    private Database db;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(4);
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
    }

    @After
    public void afterEach() {
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
    }

    @Test
    public void testTableCreate() {
        Schema s = TestUtils.createSchemaWithAllTypes();

        try(Transaction t = this.db.beginTransaction()) {
            t.createTable(s, "testTable1");
        }
    }

    @Test
    public void testTransactionBegin() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            RecordId rid = t1.getTransactionContext().addRecord(tableName, input);
            t1.getTransactionContext().getRecord(tableName, rid);
        }
    }

    @Test
    public void testTransactionTempTable() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            RecordId rid = t1.getTransactionContext().addRecord(tableName, input);
            Record rec = t1.getTransactionContext().getRecord(tableName, rid);
            assertEquals(input, rec);

            String tempTableName = t1.getTransactionContext().createTempTable(s);
            rid = t1.getTransactionContext().addRecord(tempTableName, input);
            rec = t1.getTransactionContext().getRecord(tempTableName, rid);
            assertEquals(input, rec);
        }
    }

    @Test(expected = DatabaseException.class)
    public void testTransactionTempTable2() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        RecordId rid;
        Record rec;
        String tempTableName;
        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            rid = t1.getTransactionContext().addRecord(tableName, input);
            rec = t1.getTransactionContext().getRecord(tableName, rid);
            assertEquals(input, rec);

            tempTableName = t1.getTransactionContext().createTempTable(s);
            rid = t1.getTransactionContext().addRecord(tempTableName, input);
            rec = t1.getTransactionContext().getRecord(tempTableName, rid);
            assertEquals(input, rec);
        }

        try(Transaction t2 = db.beginTransaction()) {
            t2.insert(tempTableName, input);
        }
    }

    @Test
    public void testDatabaseDurability() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        RecordId rid;
        Record rec;
        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            rid = t1.getTransactionContext().addRecord(tableName, input);
            rec = t1.getTransactionContext().getRecord(tableName, rid);

            assertEquals(input, rec);
        }

        db.close();
        db = new Database(this.filename, 32);

        try(Transaction t1 = db.beginTransaction()) {
            rec = t1.getTransactionContext().getRecord(tableName, rid);
            assertEquals(input, rec);
        }
    }

    @Test
    public void testREADMESample() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            t1.createTable(s, "table1");
            t1.insert("table1",1, "Jane", "Doe");
            t1.insert("table1", 2, "John", "Doe");
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            Iterator<Record> iter = t2.query("table1").execute();
            assertEquals(new Record(1, "Jane", "Doe"), iter.next());
            assertEquals(new Record(2, "John", "Doe"), iter.next());
            assertFalse(iter.hasNext());
            t2.commit();
        }
    }

    @Test
    public void testJoinQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema()
                .add("id", Type.intType())
                .add("firstName", Type.stringType(10))
                .add("lastName", Type.stringType(10));
            t1.createTable(s, "table1");
            t1.insert("table1", 1, "Jane", "Doe");
            t1.insert("table1", 2, "John", "Doe");
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // FROM table1 AS t1
            QueryPlan queryPlan = t2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table1", "t2", "t1.lastName", "t2.lastName");
            // WHERE t1.firstName = 'Jane'
            queryPlan.select("t1.firstName", PredicateOperator.EQUALS, "Jane");
            // .. AND t2.firstName = 'John'
            queryPlan.select("t2.firstName", PredicateOperator.EQUALS, "John");
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.project("t1.id", "t2.id", "t1.firstName", "t2.firstName", "t1.lastName");
            // run the query
            Iterator<Record> iter = queryPlan.execute();

            assertEquals(new Record(1, 2, "Jane", "John", "Doe"), iter.next());

            assertFalse(iter.hasNext());

            t2.commit();
        }
    }

    @Test
    public void testAggQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema()
                .add("id", Type.intType())
                .add("firstName", Type.stringType(10))
                .add("lastName", Type.stringType(10));
            t1.createTable(s, "table1");
            t1.insert("table1", 1, "Jack", "Doe");
            t1.insert("table1", 2, "John", "Doe");
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            //  SELECT COUNT(*), SUM(id), AVG(id) FROM table1;
            QueryPlan queryPlan = t2.query("table1");
            queryPlan.project("COUNT(*)", "SUM(id)", "AVG(id)");

            // run the query
            Iterator<Record> iter = queryPlan.execute();
            assertEquals(new Record(2, 3, 1.5f), iter.next());
            assertFalse(iter.hasNext());

            t2.commit();
        }
    }

    @Test
    public void testGroupByQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            t1.createTable(s, "table1");
            t1.insert("table1", 1, "Jane", "Doe");
            t1.insert("table1", 2, "John", "Doe");
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // SELECT lastName, COUNT(*) FROM table1 GROUP BY lastName;
            QueryPlan queryPlan = t2.query("table1");
            queryPlan.project("lastName", "COUNT(*)");
            queryPlan.groupBy("lastName");

            // run the query
            Iterator<Record> iter = queryPlan.execute();
            assertEquals(new Record("Doe", 2), iter.next());
            assertFalse(iter.hasNext());
            t2.commit();
        }
    }

    @Test
    public void testUpdateQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            t1.createTable(s, "table1");
            t1.insert("table1", 1, "Jack", "Doe");
            t1.insert("table1", 2, "John", "Doe");

            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // UPDATE table1 SET id = id + 10 WHERE lastName = 'Doe'
            t2.update("table1", "id", (DataBox x) -> new IntDataBox(x.getInt() + 10),
                      "lastName", PredicateOperator.EQUALS, new StringDataBox("Doe", 10));

            Iterator<Record> iter = t2.query("table1").execute();
            assertEquals(new Record(11, "Jack", "Doe"), iter.next());
            assertEquals(new Record(12, "John", "Doe"), iter.next());
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testDeleteQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema()
                    .add("id", Type.intType())
                    .add("firstName", Type.stringType(10))
                    .add("lastName", Type.stringType(10));
            t1.createTable(s, "table1");
            t1.insert("table1", 1, "Jane", "Doe");
            t1.insert("table1", 2, "John", "Doe");
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // DELETE FROM table1 WHERE id <> 2;
            t2.delete("table1", "id", PredicateOperator.NOT_EQUALS, new IntDataBox(2));
            Iterator<Record> iter = t2.query("table1").execute();
            assertEquals(new Record(2, "John", "Doe"), iter.next());
            assertFalse(iter.hasNext());
        }
    }
}
