package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.join.BNLJOperator;
import edu.berkeley.cs186.database.query.join.PNLJOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * These are extra tests that *won't be graded*, but should help pinpoint bugs in
 * the tests in TestNestedLoopJoins with excessively detailed output when something
 * messes up.
 */
public class ExtraNLJTests {
    // All of these tests use 4 records per page. This record size forces the
    // tables to have 4 records per page. See the calculation in
    // Table#computeNumRecordsPerPage for more details.
    private static final int RECORD_SIZE = 800;

    private Database d;
    private long numIOs;
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(5); // B=5
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        for (Page p : pinnedPages.values()) {
            p.unpin();
        }
        d.close();
    }

    // 8 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            3000 * TimeoutScaling.factor)));

    public Pair<NLJVisualizer, List<Record>> setupValues(Transaction transaction, int[] leftVals, int[] rightVals, boolean blockjoin) {
        List<Record> leftRecords = new ArrayList<>();
        List<Record> rightRecords = new ArrayList<>();

        transaction.createTable(getSchema(), "leftTable");
        transaction.createTable(getSchema(), "rightTable");
        for (int i = 0; i < leftVals.length; i++) {
            Record leftRecord = new Record("left", i/4 + 1, (i % 4) + 1, leftVals[i]);
            leftRecords.add(leftRecord);
            transaction.insert("leftTable", leftRecord);
        }
        for (int i = 0; i < rightVals.length; i++) {
            Record rightRecord = new Record("right", i/4 + 1, (i % 4) + 1, rightVals[i]);
            rightRecords.add(rightRecord);
            transaction.insert("rightTable", rightRecord);
        }

        List<Record> expectedRecords = new ArrayList<>();
        if (blockjoin) {
            for (int rp = 0; rp < rightRecords.size() / 4; rp++) {
                for (int l = 0; l < leftRecords.size(); l++) {
                    for (int r = 0; r < 4; r++) {
                        Record leftRecord = leftRecords.get(l);
                        Record rightRecord = rightRecords.get(rp*4 + r);
                        if (leftRecord.getValue(3).equals(rightRecord.getValue(3))) {
                            expectedRecords.add(leftRecord.concat(rightRecord));
                        }
                    }
                }
            }
        } else {
            for (int lp = 0; lp < leftRecords.size() / 4; lp++) {
                for (int rp = 0; rp < rightRecords.size() / 4; rp++) {
                    for (int l = 0; l < 4; l++) {
                        for (int r = 0; r < 4; r++) {
                            Record leftRecord = leftRecords.get(lp*4 + l);
                            Record rightRecord = rightRecords.get(rp*4 + r);
                            if (leftRecord.getValue(3).equals(rightRecord.getValue(3))) {
                                expectedRecords.add(leftRecord.concat(rightRecord));
                            }
                        }
                    }
                }
            }
        }
        NLJVisualizer viz = new NLJVisualizer(
                leftVals.length / 4, rightVals.length / 4, leftRecords, rightRecords, expectedRecords
        );
        setSourceOperators(
                new SequentialScanOperator(transaction.getTransactionContext(), "leftTable"),
                new SequentialScanOperator(transaction.getTransactionContext(), "rightTable")
        );
        return new Pair(viz, expectedRecords);
    }

    @Test
    public void test1x1PNLJFull() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         0
        //    "left" |       1 |         2 |         0
        //    "left" |       1 |         3 |         0
        //    "left" |       1 |         4 |         0
        //
        // and here's what the right table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         0
        //   "right" |       1 |         2 |         0
        //   "right" |       1 |         3 |         0
        //   "right" |       1 |         4 |         0
        //
        // The tables will be joined on the last column, which is always 0.
        // Since this is an equijoin we expect every pair of records to be
        // returned.
        //
        //         +---------+
        // Left  0 | x x x x |
        // Page  0 | x x x x |
        //  #1   0 | x x x x |
        //       0 | x x x x |
        //         +---------+
        //           0 0 0 0
        //           Right
        //           Page #1
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {0, 0, 0, 0};
            int[] rightVals = {0, 0, 0, 0};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);
            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);
            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test1x1PNLJPartialA() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         2
        //    "left" |       1 |         4 |         2
        //
        // and here's what the right table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         1
        //   "right" |       1 |         2 |         1
        //   "right" |       1 |         3 |         2
        //   "right" |       1 |         4 |         2
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        //
        //         +---------+
        // Left  2 |     x x |
        // Page  2 |     x x |
        //  #1   1 | x x     |
        //       1 | x x     |
        //         +---------+
        //           1 1 2 2
        //           Right
        //           Page #1
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {1, 1, 2, 2};
            int[] rightVals = {1, 1, 2, 2};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test1x1PNLJPartialB() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         2
        //    "left" |       1 |         4 |         2
        //
        // and here's what the right table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         2
        //   "right" |       1 |         2 |         2
        //   "right" |       1 |         3 |         1
        //   "right" |       1 |         4 |         1
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        //
        //         +---------+
        // Left  2 | x x     |
        // Page  2 | x x     |
        // #1    1 |     x x |
        //       1 |     x x |
        //         +---------+
        //           2 2 1 1
        //           Right
        //           Page #1
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {1, 1, 2, 2};
            int[] rightVals = {2, 2, 1, 1};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2PNLJFull() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         0
        //    "left" |       1 |         2 |         0
        //    "left" |       1 |         3 |         0
        //    "left" |       1 |         4 |         0
        //    "left" |       2 |         1 |         0
        //    "left" |       2 |         2 |         0
        //    "left" |       2 |         3 |         0
        //    "left" |       2 |         4 |         0
        //
        // The right table looks identical but with the name changed to "right"
        //
        // The tables will be joined on the last column, which is always 0.
        // Since this is an equijoin we expect every pair of records to be
        // returned.
        //
        //         +---------+---------+
        // Left  0 | x x x x | x x x x |
        // Page  0 | x x x x | x x x x |
        // #2    0 | x x x x | x x x x |
        //       0 | x x x x | x x x x |
        //         +---------+---------+
        // Left  0 | x x x x | x x x x |
        // Page  0 | x x x x | x x x x |
        // #1    0 | x x x x | x x x x |
        //       0 | x x x x | x x x x |
        //         +---------+---------+
        //           0 0 0 0    0 0 0 0
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {0, 0, 0, 0, 0, 0, 0, 0};
            int[] rightVals = {0, 0, 0, 0, 0, 0, 0, 0};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2PNLJPartialA() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         1
        //    "left" |       1 |         4 |         1
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         2
        //    "left" |       2 |         4 |         2
        //
        // The right table looks identical but with the name changed to "right"
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  2 |         | x x x x |
        // Page  2 |         | x x x x |
        // #2    2 |         | x x x x |
        //       2 |         | x x x x |
        //         +---------+---------+
        // Left  1 | x x x x |         |
        // Page  1 | x x x x |         |
        // #1    1 | x x x x |         |
        //       1 | x x x x |         |
        //         +---------+---------+
        //           1 1 1 1    2 2 2 2
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {1, 1, 1, 1, 2, 2, 2, 2};
            int[] rightVals = {1, 1, 1, 1, 2, 2, 2, 2};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2PNLJPartialB() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         1
        //    "left" |       1 |         4 |         1
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         2
        //    "left" |       2 |         4 |         2
        //
        // The right table looks like the this:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         2
        //   "right" |       1 |         2 |         2
        //   "right" |       1 |         3 |         2
        //   "right" |       1 |         4 |         2
        //   "right" |       2 |         1 |         1
        //   "right" |       2 |         2 |         1
        //   "right" |       2 |         3 |         1
        //   "right" |       2 |         4 |         1
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  2 | x x x x |         |
        // Page  2 | x x x x |         |
        // #2    2 | x x x x |         |
        //       2 | x x x x |         |
        //         +---------+---------+
        // Left  1 |         | x x x x |
        // Page  1 |         | x x x x |
        // #1    1 |         | x x x x |
        //       1 |         | x x x x |
        //         +---------+---------+
        //           2 2 2 2    1 1 1 1
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {1, 1, 1, 1, 2, 2, 2, 2};
            int[] rightVals = {2, 2, 2, 2, 1, 1, 1, 1};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2PNLJPartialC() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         2
        //    "left" |       1 |         4 |         2
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         1
        //    "left" |       2 |         4 |         1
        //
        // The right table looks like the this:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         1
        //   "right" |       1 |         2 |         1
        //   "right" |       1 |         3 |         2
        //   "right" |       1 |         4 |         2
        //   "right" |       2 |         1 |         2
        //   "right" |       2 |         2 |         2
        //   "right" |       2 |         3 |         1
        //   "right" |       2 |         4 |         1
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  1 | x x     |     x x |
        // Page  1 | x x     |     x x |
        // #2    2 |     x x | x x     |
        //       2 |     x x | x x     |
        //         +---------+---------+
        // Left  2 |     x x | x x     |
        // Page  2 |     x x | x x     |
        // #1    1 | x x     |     x x |
        //       1 | x x     |     x x |
        //         +---------+---------+
        //           1 1 2 2    2 2 1 1
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {1, 1, 2, 2, 2, 2, 1, 1};
            int[] rightVals = {1, 1, 2, 2, 2, 2, 1, 1};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2PNLJPartialD() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         2
        //    "left" |       1 |         4 |         2
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         1
        //    "left" |       2 |         4 |         1
        //
        // The right table looks like the this:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         2
        //   "right" |       1 |         2 |         2
        //   "right" |       1 |         3 |         1
        //   "right" |       1 |         4 |         1
        //   "right" |       2 |         1 |         1
        //   "right" |       2 |         2 |         1
        //   "right" |       2 |         3 |         2
        //   "right" |       2 |         4 |         2
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  1 |     x x | x x     |
        // Page  1 |     x x | x x     |
        // #2    2 | x x     |     x x |
        //       2 | x x     |     x x |
        //         +---------+---------+
        // Left  2 | x x     |     x x |
        // Page  2 | x x     |     x x |
        // #1    1 |     x x | x x     |
        //       1 |     x x | x x     |
        //         +---------+---------+
        //           2 2 1 1    1 1 2 2
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(5); // B=5
            int[] leftVals = {1, 1, 2, 2, 2, 2, 1, 1};
            int[] rightVals = {2, 2, 1, 1, 1, 1, 2, 2};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, false);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2BNLJFull() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         0
        //    "left" |       1 |         2 |         0
        //    "left" |       1 |         3 |         0
        //    "left" |       1 |         4 |         0
        //    "left" |       2 |         1 |         0
        //    "left" |       2 |         2 |         0
        //    "left" |       2 |         3 |         0
        //    "left" |       2 |         4 |         0
        //
        // The right table looks identical but with the name changed to "right"
        //
        // The tables will be joined on the last column, which is always 0.
        // Since this is an equijoin we expect every pair of records to be
        // returned.
        //
        //         +---------+---------+
        // Left  0 | x x x x | x x x x |
        // Page  0 | x x x x | x x x x |
        // #2    0 | x x x x | x x x x |
        //       0 | x x x x | x x x x |
        //         +---------+---------+
        // Left  0 | x x x x | x x x x |
        // Page  0 | x x x x | x x x x |
        // #1    0 | x x x x | x x x x |
        //       0 | x x x x | x x x x |
        //         +---------+---------+
        //           0 0 0 0    0 0 0 0
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(4); // B=4
            int[] leftVals = {0, 0, 0, 0, 0, 0, 0, 0};
            int[] rightVals = {0, 0, 0, 0, 0, 0, 0, 0};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, true);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(3); // 2 IOs to load in the first left block, 1 IO to load in the first right page

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2BNLJPartialA() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         1
        //    "left" |       1 |         4 |         1
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         2
        //    "left" |       2 |         4 |         2
        //
        // The right table looks identical but with the name changed to "right"
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  2 |         | x x x x |
        // Page  2 |         | x x x x |
        // #2    2 |         | x x x x |
        //       2 |         | x x x x |
        //         +---------+---------+
        // Left  1 | x x x x |         |
        // Page  1 | x x x x |         |
        // #1    1 | x x x x |         |
        //       1 | x x x x |         |
        //         +---------+---------+
        //           1 1 1 1    2 2 2 2
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(4); // B=4
            int[] leftVals = {1, 1, 1, 1, 2, 2, 2, 2};
            int[] rightVals = {1, 1, 1, 1, 2, 2, 2, 2};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, true);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(3); // 2 IOs to load in the first left block, 1 IO to load in the first right page

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2BNLJPartialB() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         1
        //    "left" |       1 |         4 |         1
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         2
        //    "left" |       2 |         4 |         2
        //
        // The right table looks like the this:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         2
        //   "right" |       1 |         2 |         2
        //   "right" |       1 |         3 |         2
        //   "right" |       1 |         4 |         2
        //   "right" |       2 |         1 |         1
        //   "right" |       2 |         2 |         1
        //   "right" |       2 |         3 |         1
        //   "right" |       2 |         4 |         1
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  2 | x x x x |         |
        // Page  2 | x x x x |         |
        // #2    2 | x x x x |         |
        //       2 | x x x x |         |
        //         +---------+---------+
        // Left  1 |         | x x x x |
        // Page  1 |         | x x x x |
        // #1    1 |         | x x x x |
        //       1 |         | x x x x |
        //         +---------+---------+
        //           2 2 2 2    1 1 1 1
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(4); // B=4
            int[] leftVals = {1, 1, 1, 1, 2, 2, 2, 2};
            int[] rightVals = {2, 2, 2, 2, 1, 1, 1, 1};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, true);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(3); // 2 IOs to load in the first left block, 1 IO to load in the first right page

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2BNLJPartialC() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         2
        //    "left" |       1 |         4 |         2
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         1
        //    "left" |       2 |         4 |         1
        //
        // The right table looks like the this:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         1
        //   "right" |       1 |         2 |         1
        //   "right" |       1 |         3 |         2
        //   "right" |       1 |         4 |         2
        //   "right" |       2 |         1 |         2
        //   "right" |       2 |         2 |         2
        //   "right" |       2 |         3 |         1
        //   "right" |       2 |         4 |         1
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  1 | x x     |     x x |
        // Page  1 | x x     |     x x |
        // #2    2 |     x x | x x     |
        //       2 |     x x | x x     |
        //         +---------+---------+
        // Left  2 |     x x | x x     |
        // Page  2 |     x x | x x     |
        // #1    1 | x x     |     x x |
        //       1 | x x     |     x x |
        //         +---------+---------+
        //           1 1 2 2    2 2 1 1
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(4); // B=4
            int[] leftVals = {1, 1, 2, 2, 2, 2, 1, 1};
            int[] rightVals = {1, 1, 2, 2, 2, 2, 1, 1};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, true);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(3); // 2 IOs to load in the first left block, 1 IO to load in the first right page

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    @Test
    public void test2x2BNLJPartialD() {
        // Constructs two tables that are both 1 page each. Each page contains
        // exactly four records. The schema of each record is
        // (STRING, INT, INT, INT). Here's what the left table looks like:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //    "left" |       1 |         1 |         1
        //    "left" |       1 |         2 |         1
        //    "left" |       1 |         3 |         2
        //    "left" |       1 |         4 |         2
        //    "left" |       2 |         1 |         2
        //    "left" |       2 |         2 |         2
        //    "left" |       2 |         3 |         1
        //    "left" |       2 |         4 |         1
        //
        // The right table looks like the this:
        //
        // tableName | pageNum | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |       1 |         1 |         2
        //   "right" |       1 |         2 |         2
        //   "right" |       1 |         3 |         1
        //   "right" |       1 |         4 |         1
        //   "right" |       2 |         1 |         1
        //   "right" |       2 |         2 |         1
        //   "right" |       2 |         3 |         2
        //   "right" |       2 |         4 |         2
        //
        // The tables will be joined on the last column, which is either 1 or 2.
        // Since this is an equijoin we expect the following records to be returned:
        //
        //         +---------+---------+
        // Left  1 |     x x | x x     |
        // Page  1 |     x x | x x     |
        // #2    2 | x x     |     x x |
        //       2 | x x     |     x x |
        //         +---------+---------+
        // Left  2 | x x     |     x x |
        // Page  2 | x x     |     x x |
        // #1    1 |     x x | x x     |
        //       1 |     x x | x x     |
        //         +---------+---------+
        //           2 2 1 1    1 1 2 2
        //           Right      Right
        //           Page #1    Page #2
        //
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            d.setWorkMem(4); // B=4
            int[] leftVals = {1, 1, 2, 2, 2, 2, 1, 1};
            int[] rightVals = {2, 2, 1, 1, 1, 1, 2, 2};
            Pair<NLJVisualizer, List<Record>> p = setupValues(transaction, leftVals, rightVals, true);
            NLJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            startCountIOs();
            // Constructing the operator should incur 0 IOs
            QueryOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(3); // 2 IOs to load in the first left block, 1 IO to load in the first right page

            for (int i = 0; i < expectedRecords.size() && outputIterator.hasNext(); i++) {
                viz.add(expectedRecords.get(i), outputIterator.next(), i);
            }
            StringBuilder problems = new StringBuilder(viz.getProblems());
            if (outputIterator.hasNext()) {
                problems.append("== EXTRA RECORDS ==\n");
                problems.append("You're outputting more than the expected number of records.\n");
                problems.append("Here are up to ten of the extra records:\n");
                int i = 0;
                while(i < 10 && outputIterator.hasNext()) {
                    problems.append(outputIterator.next().toString() + "\n");
                    i++;
                }
            }
            String report = problems.toString();
            if (!report.equals("")) {
                throw new RuntimeException("\n" + report);
            }
        }
    }

    // ur debugging bff
    public class NLJVisualizer {
        private int leftPages;
        private int rightPages;
        private String grid;
        private List<Record> repeats;
        private int mismatchedNum;
        private boolean[][] actual;
        private boolean[][] expected;
        private String[][] firstMismatch;
        private String[][] fullRun;

        public NLJVisualizer(int leftPages, int rightPages, List<Record> leftRecords, List<Record> rightRecords, List<Record> expectedOutput) {
            this.mismatchedNum = -1;
            this.leftPages = leftPages;
            this.rightPages = rightPages;
            this.repeats = new ArrayList<>();
            List<String> rows = new ArrayList<>();
            for (int i = leftPages; i > 0; i--) {
                rows.add(createSeparator());
                for (int j = 4; j >= 1; j--) {
                    String prefix;
                    if (j == 4) prefix = " Left  ";
                    else if (j == 3) prefix = " Page  ";
                    else if (j == 2) prefix = " #"+(i)+"    ";
                    else prefix = "       ";
                    prefix += leftRecords.get(getIndex(i, j)).getValue(3).getInt() + " ";
                    rows.add(createRow(prefix));
                }
            }
            rows.add(createSeparator());
            rows.addAll(createRightLabels(rightRecords));
            this.grid = String.join("\n", rows) + "\n";

            this.firstMismatch = new String[leftPages * 4][rightPages * 4];
            this.fullRun = new String[leftPages * 4][rightPages * 4];
            for (int i = 0; i < leftPages * 4; i++) {
                for (int j = 0; j < rightPages * 4; j++) {
                    this.fullRun[i][j] = " ";
                    this.firstMismatch[i][j] = " ";
                }
            }

            this.expected = new boolean[leftPages * 4][rightPages * 4];
            this.actual = new boolean[leftPages * 4][rightPages * 4];
            for (Record r: expectedOutput) {
                int leftPage = r.getValue(1).getInt();
                int leftRecord = r.getValue(2).getInt();
                int rightPage = r.getValue(5).getInt();
                int rightRecord = r.getValue(6).getInt();
                this.expected[getIndex(leftPage,leftRecord)][getIndex(rightPage, rightRecord)] = true;
            }
        }

        public boolean isMismatched() {
            return this.mismatchedNum != -1;
        }

        private String createSeparator() {
            StringBuilder b = new StringBuilder("         ");
            for (int i = 0; i < this.rightPages; i++) {
                b.append("+---------");
            }
            b.append("+");
            return b.toString();
        }

        private String createRow(String prefix) {
            StringBuilder b = new StringBuilder(prefix);
            for (int i = 0; i < this.rightPages; i++) {
                b.append("| %s %s %s %s ");
            }
            b.append("|");
            return b.toString();
        }

        private List<String> createRightLabels(List<Record> rightRecords) {
            StringBuilder b = new StringBuilder("         ");
            StringBuilder b2 = new StringBuilder("         ");
            StringBuilder b3 = new StringBuilder("         ");
            for (int i = 0; i < this.rightPages; i++) {
                int v1 = rightRecords.get(i*4).getValue(3).getInt();
                int v2 = rightRecords.get(i*4+1).getValue(3).getInt();
                int v3 = rightRecords.get(i*4+2).getValue(3).getInt();
                int v4 = rightRecords.get(i*4+3).getValue(3).getInt();;
                b.append(String.format("  %d %d %d %d  ", v1, v2, v3, v4));
                b2.append("  Right    ");
                b3.append("  Page #" + (i+1) + "  ");
            }
            return Arrays.asList(b.toString(), b2.toString(), b3.toString());
        }

        private String visualizeState(String[][] state) {
            String[] vals = new String[this.leftPages * this.rightPages * 16];
            int pos = 0;
            for (int l = state.length - 1; l >= 0; l--) {
                String[] row = state[l];
                for (int r = 0; r < row.length; r++) {
                    vals[pos] = row[r];
                    pos++;
                }
            }
            return String.format(this.grid, vals);
        }

        private String visualizeFirstMismatch() {
            return visualizeState(this.firstMismatch);
        }

        private String visualizeFullRun() {
            return visualizeState(this.fullRun);
        }

        private boolean computeFullRun() {
            boolean problem = false;
            for (int l = 0; l < this.actual.length; l++) {
                for (int r = 0; r < this.actual[l].length; r++) {
                    boolean a = this.actual[l][r];
                    boolean e = this.expected[l][r];
                    problem |= a != e;
                    if (e) {
                        if(a) this.fullRun[l][r] = "x";
                        else  this.fullRun[l][r] = "?";
                    } else {
                        if(a) this.fullRun[l][r] = "+";
                        else this.fullRun[l][r] = " ";
                    }
                }
            }

            for (Record r: this.repeats) {
                int leftIndex = getIndex(r.getValue(1).getInt(), r.getValue(2).getInt());
                int rightIndex = getIndex(r.getValue(5).getInt(), r.getValue(6).getInt());
                this.fullRun[leftIndex][rightIndex] = "r";
            }
            return problem;
        }

        public void add(Record expectedRecord, Record actualRecord, int num) {
            assertEquals("Your output records should have 8 values. Did you join the left and right records properly?",8, actualRecord.size());
            int actualLeftPage = actualRecord.getValue(1).getInt();
            int actualLeftRecord = actualRecord.getValue(2).getInt();
            int actualRightPage = actualRecord.getValue(5).getInt();
            int actualRightRecord = actualRecord.getValue(6).getInt();

            int expectedLeftPage = expectedRecord.getValue(1).getInt();
            int expectedLeftRecord = expectedRecord.getValue(2).getInt();
            int expectedRightPage = expectedRecord.getValue(5).getInt();
            int expectedRightRecord = expectedRecord.getValue(6).getInt();

            int expectedLeftIndex = getIndex(expectedLeftPage, expectedLeftRecord);
            int expectedRightIndex = getIndex(expectedRightPage, expectedRightRecord);
            int actualLeftIndex = getIndex(actualLeftPage, actualLeftRecord);
            int actualRightIndex = getIndex(actualRightPage, actualRightRecord);

            if (!expectedRecord.equals(actualRecord)) {
                if (!this.isMismatched()) {
                    this.mismatchedNum = num+1;
                    this.firstMismatch[expectedLeftIndex][expectedRightIndex] = "E";
                    this.firstMismatch[actualLeftIndex][actualRightIndex] = "A";
                }
            }

            if (!this.isMismatched()) {
                this.firstMismatch[actualLeftIndex][actualRightIndex] = "x";
            }

            if (this.actual[actualLeftIndex][actualRightIndex]) {
                this.repeats.add(actualRecord);
            }
            this.actual[actualLeftIndex][actualRightIndex] = true;
        }

        private int getIndex(int pageNum, int recordNum) {
            return (pageNum-1)*4 + recordNum-1;
        }

        public String getProblems() {
            StringBuilder b = new StringBuilder();
            if (this.isMismatched()) {
                b.append("== MISMATCH == \n");
                b.append(visualizeFirstMismatch() + "\n");
                b.append("You had 1 or more mismatched records. The first mismatch \n");
                b.append("was at record #" + this.mismatchedNum + ". The above shows the state of \n");
                b.append("the join when the mismatch occurred. Key:\n");
                b.append(" - x means your join properly yielded this record at the right time\n");
                b.append(" - E was the record we expected you to yield\n");
                b.append(" - A was the record that you actually yielded\n\n");
            }

            if(computeFullRun()) {
                b.append("== MISSING OR EXTRA RECORDS == \n");;
                b.append(visualizeFullRun());
                b.append("\n");
                b.append("You either excluded or included records when you shouldn't have. Key:\n");
                b.append(" - x means we expected this record to be included and you included it\n");
                b.append(" - + means we expected this record to be excluded and you included it\n");
                b.append(" - ? means we expected this record to be included and you excluded it\n");
                b.append(" - r means you included this record multiple times\n");
                b.append(" - a blank means we expected this record to be excluded and you excluded it\n\n");
            }

            if (this.repeats.size() > 0) {
                b.append("== REPEATS ==\n");
                b.append("You yielded the following records multiple times:\n");
                for (Record repeat: this.repeats) {
                    b.append(repeat.toString() + "\n");
                }
                b.append("\n");
            }

            return b.toString();
        }
    }

    // Helpers
    private void startCountIOs() {
        d.getBufferManager().evictAll();
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void checkIOs(String message, long minIOs, long maxIOs) {
        if (message == null) {
            message = "";
        } else {
            message = "(" + message + ")";
        }

        long newIOs = d.getBufferManager().getNumIOs();
        long IOs = newIOs - numIOs;

        assertTrue(IOs + " I/Os not between " + minIOs + " and " + maxIOs + message,
                minIOs <= IOs && IOs <= maxIOs);
        numIOs = newIOs;
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

    private void unpinPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        this.pinnedPages.remove(pnum).unpin();
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
        pinPage(1, 1); // _metadata.tables entry for left source
        pinPage(1, 2); // _metadata.tables entry for right source
        pinPage(3, 0); // left source header page
        pinPage(4, 0); // right source header page
    }

    public Schema getSchema() {
        return new Schema()
                .add("table", Type.stringType(RECORD_SIZE))
                .add("pageNum", Type.intType())
                .add("recordNum", Type.intType())
                .add("joinValue", Type.intType());
    }
}
