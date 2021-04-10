package edu.berkeley.cs186.database.cli;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.cli.parser.ASTSQLStatementList;
import edu.berkeley.cs186.database.cli.parser.ParseException;
import edu.berkeley.cs186.database.cli.parser.RookieParser;
import edu.berkeley.cs186.database.cli.visitor.SelectStatementVisitor;
import edu.berkeley.cs186.database.cli.visitor.StatementListVisitor;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({Proj99Tests.class, SystemTests.class})
public class TestSelectClause {
    private static final String TestDir = "testSelectClause";
    private Database db;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(16);
        try {
            db.loadDemo();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @After
    public void afterEach() {
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
    }

    public SelectStatementVisitor parse(String input) {
        RookieParser p = new RookieParser(new ByteArrayInputStream(input.getBytes()));
        ASTSQLStatementList node;
        try {
            node = p.sql_stmt_list();
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
        StatementListVisitor visitor = new StatementListVisitor(db);
        node.jjtAccept(visitor, null);
        return (SelectStatementVisitor) visitor.statementVisitors.get(0);
    }

    @Test
    public void testSimpleSelect() {
        SelectStatementVisitor v = parse(
                "SELECT sid FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("sid"));
            for (int i = 1; i <= 200; i++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), i);
            }
        }
    }

    @Test
    public void testMultiColumnSelect() {
        SelectStatementVisitor v = parse(
                "SELECT sid, gpa, major FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("sid", "gpa", "major"));
            for (int i = 1; i <= 200; i++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), i);
            }
        }
    }

    @Test
    public void testAsteriskSelect() {
        SelectStatementVisitor v = parse(
                "SELECT * FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("sid", "name", "major", "gpa"));
            for (int i = 1; i <= 200; i++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), i);
            }
        }
    }

    @Test
    public void testTableAsteriskSelect() {
        SelectStatementVisitor v = parse(
                "SELECT Students.* FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("Students.sid", "Students.name", "Students.major", "Students.gpa"));
            for (int i = 1; i <= 200; i++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), i);
            }
        }
    }

    @Test
    public void testAsteriskColumnSelect() {
        SelectStatementVisitor v = parse(
                "SELECT sid, *, sid FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("sid", "sid", "name", "major", "gpa", "sid"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), sid);
                assertEquals(r.getValue(1).getInt(), sid);
                assertEquals(r.getValue(5).getInt(), sid);
            }
        }
    }

    @Test
    public void testColumnAliasSelect() {
        SelectStatementVisitor v = parse(
                "SELECT sid as coolAlias FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("coolAlias"));
            for (int i = 1; i <= 200; i++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), i);
            }
        }
    }

    @Test
    public void testSimpleArithmetic() {
        SelectStatementVisitor v = parse(
                "SELECT sid + sid as sum FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("sum"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), sid + sid);
            }
        }
    }

    @Test
    public void testArithmeticMultiply() {
        SelectStatementVisitor v = parse(
                "SELECT sid * sid + sid as A, sid + sid * sid as B FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("A", "B"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), sid * sid + sid);
                assertEquals(r.getValue(1).getInt(), sid + sid * sid);
            }
        }
    }

    @Test
    public void testMultiColumnArithmetic() {
        SelectStatementVisitor v = parse(
                "SELECT gpa * sid + gpa / sid / 186 as expr, gpa FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("expr", "gpa"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                float gpa = r.getValue(1).getFloat();
                assertEquals(r.getValue(0).getFloat(), gpa * sid + gpa / sid / 186, 0.01);
            }
        }
    }

    @Test
    public void testArithmeticOrderOfOperations() {
        SelectStatementVisitor v = parse(
                "SELECT sid * 6 / (8 + sid * 1) as expr FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("expr"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), sid * 6 / (8 + sid * 1));
            }
        }
    }

    @Test
    public void testSimpleFunction() {
        SelectStatementVisitor v = parse(
                "SELECT NEGATE(sid) as fcall FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("fcall"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), -sid);
            }
        }
    }

    @Test
    public void testExpressionInFunction() {
        SelectStatementVisitor v = parse(
                "SELECT NEGATE(sid * sid + 186) as fcall FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("fcall"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), -(sid * sid + 186));
            }
        }
    }

    @Test
    public void testFunctionInExpression() {
        SelectStatementVisitor v = parse(
                "SELECT sid*NEGATE(sid)*sid as fcall FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("fcall"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), sid * -sid * sid);
            }
        }
    }

    @Test
    public void testNestedFunction() {
        SelectStatementVisitor v = parse(
                "SELECT NEGATE(NEGATE(sid)) as fcall FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("fcall"));
            for (int sid = 1; sid <= 200; sid++) {
                Record r = records.next();
                schema.verify(r);
                assertEquals(r.getValue(0).getInt(), sid);
            }
        }
    }

    @Test
    public void testMultiArgumentFunction() {
        SelectStatementVisitor v = parse(
                "SELECT major, REPLACE(UPPER(major), 'I', 'III') as fcall FROM Students;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("major", "fcall"));
            for (int i = 1; i <= 200; i++) {
                Record r = records.next();
                String major = r.getValue(0).getString();
                String fcall = r.getValue(1).getString();
                assertEquals(major.toUpperCase().replace("I", "III"), fcall);
            }
        }
    }

    @Test
    public void testAliasedAsteriskA() {
        SelectStatementVisitor v = parse(
                "SELECT A.* FROM Students AS A;"
        );
        try (Transaction t = db.beginTransaction()) {
            QueryPlan queryPlan = v.getQueryPlan(t).get();
            queryPlan.execute();
            Schema schema = queryPlan.getFinalOperator().getSchema();
            assertEquals(schema.getFieldNames(), Arrays.asList("A.sid", "A.name", "A.major", "A.gpa"));
        }
    }

    @Test
    public void testAliasedAsteriskB() {
        SelectStatementVisitor v = parse(
                "SELECT Students.* FROM Students AS B;"
        );
        try (Transaction t = db.beginTransaction()) {
            try {
                v.getQueryPlan(t).get();
                fail("Operation should have failed!");
            } catch (Exception e) {
                // do nothing
            }
        }
    }
}
