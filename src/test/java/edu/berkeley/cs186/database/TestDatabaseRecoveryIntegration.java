package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * These tests are *not* graded and not part of your project 5 submission, but
 * they should work if you finish project 5.
 */
public class TestDatabaseRecoveryIntegration {
    private static final String TestDir = "testDatabaseRecovery";
    private Database db;
    private LockManager lockManager;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 10 second max per method tested.
    public static long timeout = (long) (10000 * TimeoutScaling.factor);

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis(timeout));

    private void reloadDatabase(boolean closeOld) {
        if (closeOld && this.db != null) {
            this.db.close();
        }
        this.lockManager = new LockManager();
        this.db = new Database(this.filename, 128, this.lockManager, new ClockEvictionPolicy(), true);
        this.db.setWorkMem(32); // B=32

        if (closeOld) {
            try {
                this.db.loadDemo();
            } catch (IOException e) {
                throw new DatabaseException("Failed to load demo tables.\n" + e.getMessage());
            }
        }
        // force initialization to finish before continuing
        this.db.waitAllTransactions();
    }

    private void reloadDatabase() {
        reloadDatabase(true);
    }

    @ClassRule
    public static  TemporaryFolder checkFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.reloadDatabase();
        this.db.waitAllTransactions();
    }

    @Test
    public void testRollbackDropTable() {
        /**
         * Tests that rollbacks work properly when a table is dropped. Does a
         * full scan of `Students` table and stores records. Then, drops the
         * `Students` table. After dropping, queries to the `Students` table
         * should fail. Afterwards, the transaction is rolled back.
         *
         * In a new transaction, a new full scan of `Students` is done to
         * verify that all the records were restored.
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // Do a full scan of `Students`
        try (Transaction t= db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.dropTable("Students");
            try {
                t.query("Students").execute();
                fail("Query should have failed, Students was dropped!");
            } catch (DatabaseException e) {
                // Make sure fails for correct reason
                assertTrue(e.getMessage().contains("does not exist!"));
            }
            t.rollback();
        }

        try (Transaction t= db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRollbackDropAllTables() {
        /**
         * Identical to the above test, but instead checks the behavior
         * of dropping all tables.
         */
        String[] tableNames = {"Students", "Enrollments", "Courses"};
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // Do a full scan of each table
        try (Transaction t= db.beginTransaction()) {
            for (int i = 0; i < tableNames.length; ++i) {
                Iterator<Record> records = t.query(tableNames[i]).execute();
                while (records.hasNext()) oldRecords.add(records.next());
            }
            // Drop all the tables
            t.dropAllTables();
            for (int i = 0; i < tableNames.length; ++i) {
                try {
                    t.query(tableNames[i]).execute();
                    fail("Query should have failed, all tables were dropped!");
                } catch (DatabaseException e) {
                    // Make sure fails for correct reason
                    assertTrue(e.getMessage().contains("does not exist!"));
                }
            }
            t.rollback();
        }

        try (Transaction t= db.beginTransaction()) {
            for (int i = 0; i < tableNames.length; ++i) {
                Iterator<Record> records = t.query(tableNames[i]).execute();
                while (records.hasNext()) newRecords.add(records.next());
            }
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRollbackDropIndex() {
        /**
         * Creates an index on `Students.sid`. Since an index is available,
         * querying for equality on `sid` should use an index scan to access
         * the table in the query plan. The index is then dropped, and the query
         * plan should no longer have access to index scans. Afterwards, the
         * DROP INDEX is rolled back, querying should once again rely on an
         * index scan.
         */
        try (Transaction t = db.beginTransaction()) {
            t.createIndex("Students", "sid", false);
        }

        try (Transaction t = db.beginTransaction()) {
            QueryPlan p1 = t.query("Students");
            p1.select("sid", PredicateOperator.EQUALS, 186);
            p1.execute();
            assertTrue(p1.getFinalOperator().toString().contains("Index Scan"));
            t.dropIndex("Students", "sid");
            QueryPlan p2 = t.query("Students");
            p2.select("sid", PredicateOperator.EQUALS, 186);
            p2.execute();
            assertFalse(p2.getFinalOperator().toString().contains("Index Scan"));
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            QueryPlan p3 = t.query("Students");
            p3.select("sid", PredicateOperator.EQUALS, 186);
            p3.execute();
            assertTrue(p3.getFinalOperator().toString().contains("Index Scan"));
        }
        this.db.close();
    }

    @Test
    public void testRollbackUpdate() {
        /**
         * Does a full scan on the `Students` table and saves results for later
         * comparison. The `gpa` field for every student is then set to 0.0,
         * and another full scan is done to verify that the changes were made.
         * Finally, the changes are rolled back and the state of `Students`
         * before and after the transaction that was rolled back is compared.
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            // Flatten the curve
            t.update("Students", "gpa", (DataBox d) -> DataBox.fromObject(0.0));
            Schema s = t.getSchema("Students");
            records = t.query("Students").execute();
            while (records.hasNext()) {
                assertEquals(DataBox.fromObject(0.0), records.next().getValue(s.findField("gpa")));
            }
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
    }

    @Test
    public void testRollbackDeleteAll() {
        /**
         * Same as above, but instead of updating records deletes them.
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.delete("Students", "gpa", PredicateOperator.GREATER_THAN_EQUALS, DataBox.fromObject(0.0));
            records = t.query("Students").execute();
            assertFalse(records.hasNext());
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
    }

    @Test
    public void testRollbackDeletePartial() {
        /**
         * Same as above, but only deletes specific records.
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.delete("Students", "gpa", PredicateOperator.GREATER_THAN_EQUALS, DataBox.fromObject(2.0));
            Schema s = t.getSchema("Students");
            records = t.query("Students").execute();
            while (records.hasNext()) {
                assertTrue(records.next().getValue(s.findField("gpa")).getFloat() < 2.0);
            }
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
    }

    @Test
    public void testSavepointDropTable() {
        /**
         * Tests the functionality of save points when a table is dropped.
         * First, a full scan of `Enrollments` is done and the records are
         * stored for later comparison.
         *
         * Next, the Students table is dropped, and a savepoint is created.
         * The `Enrollments` table is dropped, then we immediately rollback
         * to the savepoint. Since we've rolled back to before `Enrollments`
         * was dropped, the table should be restored and all the original
         * records should be present when we perform another full scan.
         *
         * Finally, in a new transaction, we check that the `Students` table no
         * longer exists since we didn't rollback dropping `Students`, and we
         * check again that all the original records in `Enrollments` are
         * present.
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // Do a full scan of `Enrollments`
        try (Transaction t= db.beginTransaction()) {
            Iterator<Record> records = t.query("Enrollments").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.dropTable("Students");
            t.savepoint("beforeDroppingEnrollments");
            t.dropTable("Enrollments");
            try {
                t.query("Enrollments").execute();
                fail("Query should have failed, Enrollments was dropped!");
            } catch (DatabaseException e) {
                // Make sure fails for correct reason
                assertTrue(e.getMessage().contains("does not exist!"));
            }
            t.rollbackToSavepoint("beforeDroppingEnrollments");
            List<Record> afterRollbackRecords = new ArrayList<>();
            records = t.query("Enrollments").execute();
            while (records.hasNext()) afterRollbackRecords.add(records.next());
            assertEquals(oldRecords, afterRollbackRecords);
        }

        try (Transaction t= db.beginTransaction()) {
            // `DROP TABLE Students` wasn't rolled back, should still fail when
            // attempting to query.
            try {
                t.query("Students").execute();
                fail("Query should have failed, Students was dropped!");
            } catch (DatabaseException e) {
                // Make sure fails for correct reason
                assertTrue(e.getMessage().contains("does not exist!"));
            }

            Iterator<Record> records = t.query("Enrollments").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRebootDropTable() {
        /**
         * Simulates a database crash mid-transaction. In this case, T1 is a
         * transaction that drops the `Students` table, but does not commit
         * before the database is rebooted. T2 is a new transaction created
         * after the database recovers, so it should be able to access the
         * `Students` table as though it were never dropped at all.
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // Do a full scan of `Students`
        Transaction t1 = db.beginTransaction();
        Iterator<Record> records = t1.query("Students").execute();
        while (records.hasNext()) oldRecords.add(records.next());

        t1.dropTable("Students");
        try {
            t1.query("Students").execute();
            fail("Query should have failed, Students was dropped!");
        } catch (DatabaseException e) {
            // Make sure fails for correct reason
            assertTrue(e.getMessage().contains("does not exist!"));
        }
        // Note: T1 never commits!
        Database old = this.db;
        reloadDatabase(false);
        try (Transaction t2 = db.beginTransaction()) {
            Iterator<Record> records2 = t2.query("Students").execute();
            while (records2.hasNext()) newRecords.add(records2.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
        t1.commit();
        old.close();
    }

    @Test
    public void testRebootPartialDelete() {
        /**
         * Same as above, but T1 instead attempted to perform a partial deletion
         * of the records in `Students`.
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // Do a full scan of `Students`
        Transaction t1 = db.beginTransaction();
        Iterator<Record> records = t1.query("Students").execute();
        while (records.hasNext()) oldRecords.add(records.next());

        t1.delete("Students", "gpa", PredicateOperator.GREATER_THAN_EQUALS, DataBox.fromObject(1.86));
        db.getBufferManager().evictAll();

        // Note: Changes flushed, but T1 never commits!
        Database old = this.db;
        reloadDatabase(false);
        try (Transaction t2 = db.beginTransaction()) {
            Iterator<Record> records2 = t2.query("Students").execute();
            while (records2.hasNext()) newRecords.add(records2.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
        t1.commit();
        old.close();
    }

    @Test
    public void testRebootCreateTable() {
        // Creates tables, commits, and then reboots
        try(Transaction t1 = db.beginTransaction()) {
            for (int i = 0; i < 3; i++) {
                t1.createTable(new Schema().add("int", Type.intType()), "ints" + i);
                for (int j = 0; j < 1024 * 5; j++) {
                    t1.insert("ints"+i, j);
                }
            }
        }
        Database old = this.db;
        reloadDatabase(false);
        try(Transaction t2 = db.beginTransaction()) {
            for (int i = 0; i < 3; i++) {
                Iterator<Record> records = t2.query("ints" + i).execute();
                assertTrue(records.next().getValue(0).getInt() == 0);
            }
        }
        this.db.close();
        old.close();
    }

    @Test
    public void testRebootCreateAndDropTable() {
        // Creates tables, commits, and then reboots
        try(Transaction t1 = db.beginTransaction()) {
            for (int i = 0; i < 3; i++) {
                t1.createTable(new Schema().add("int", Type.intType()), "ints" + i);
                for (int j = 0; j < 1024 * 5; j++) {
                    t1.insert("ints"+i, j);
                }
            }
            t1.dropTable("ints0");
        }
        Database old = this.db;
        reloadDatabase(false);
        try(Transaction t2 = db.beginTransaction()) {
            for (int i = 1; i < 3; i++) {
                Iterator<Record> records = t2.query("ints" + i).execute();
                assertTrue(records.next().getValue(0).getInt() == 0);
            }
        }
        this.db.close();
        old.close();
    }
}
