package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.concurrency.DummyLockManager;
import edu.berkeley.cs186.database.databox.*;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import static org.junit.Assert.*;

@Category({Proj99Tests.class})
public class TestDatabaseQueries {
    private Database database;
    private Transaction transaction;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("myDb", "school");
        database = new Database(tempDir.getAbsolutePath(), 32, new DummyLockManager());
        database.setWorkMem(5); // B=5
        database.loadDemo();
        transaction = database.beginTransaction();
    }

    @After
    public void teardown() {
        transaction.commit();
        database.close();
    }

    @Test
    @Category(SystemTests.class)
    public void testJoinStudentNamesWithClassNames() {
        QueryPlan queryPlan = this.transaction.query("Students", "S");
        queryPlan.join("Enrollments", "E", "S.sid", "E.sid");
        queryPlan.join("Courses", "C", "E.cid", "C.cid");
        queryPlan.project("S.name", "C.name");

        Iterator<Record> recordIterator = queryPlan.execute();

        int count = 0;
        while (recordIterator.hasNext()) {
            recordIterator.next();
            count++;
        }

        assertEquals(1000, count);
    }
}
