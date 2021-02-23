package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.join.SortMergeOperator;
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
/**
 * These are extra tests that *won't be graded*, but should help pinpoint bugs in
 * the tests in TestSortMergeJoin with excessively detailed output when something
 * messes up.
 */
public class ExtraSMJTests {
    private Database d;
    private TestSourceOperator leftSourceOperator;
    private TestSourceOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("extraSmjTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(5); // B=5
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        for (Page p : pinnedPages.values()) p.unpin();
        d.close();
    }

    // 4 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            4000 * TimeoutScaling.factor)));

    private void setSourceOperators(List<Record> leftValues, List<Record> rightValues) {
        this.leftSourceOperator = new TestSourceOperator(leftValues, getTestSchema());
        this.rightSourceOperator = new TestSourceOperator(rightValues, getTestSchema());
        this.leftSourceOperator.setSortedOn("joinValue");
        this.rightSourceOperator.setSortedOn("joinValue");
    }

    public Schema getTestSchema() {
        return new Schema().add("tableName", Type.stringType(5))
                           .add("recordNum", Type.intType())
                           .add("joinValue", Type.intType());
    }

    public Pair<SMJVisualizer, List<Record>> setupValues(int[] leftVals, int[] rightVals) {
        List<Record> leftRecords = new ArrayList<>();
        List<Record> rightRecords = new ArrayList<>();
        for (int i = 0; i < leftVals.length; i++) {
            leftRecords.add(new Record("left", i + 1, leftVals[i]));
        }

        for (int i = 0; i < rightVals.length; i++) {
            rightRecords.add(new Record("right", i + 1, rightVals[i]));
        }

        List<Record> expectedRecords = new ArrayList<>();
        for (int l = 0; l < leftRecords.size(); l++) {
            for (int r = 0; r < rightRecords.size(); r++) {
                Record leftRecord = leftRecords.get(l);
                Record rightRecord = rightRecords.get(r);
                if (leftRecord.getValue(2).equals(rightRecord.getValue(2))) {
                    expectedRecords.add(leftRecord.concat(rightRecord));
                }
            }
        }
        SMJVisualizer viz = new SMJVisualizer(
                leftRecords, rightRecords, expectedRecords
        );
        setSourceOperators(leftRecords, rightRecords);
        return new Pair(viz, expectedRecords);
    }

    @Test
    public void test4x4() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         2
        //    "left" |         3 |         3
        //    "left" |         4 |         4
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         2
        //   "right" |         3 |         3
        //   "right" |         4 |         4
        //
        // We expect the following matches to occur
        //
        //         +---------+
        //       4 |       x |
        // Left  3 |     x   |
        //       2 |   x     |
        //       1 | x       |
        //         +---------+
        //           1 2 3 4
        //            Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,2,3,4};
            int[] rightValues = {1,2,3,4};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test4x4SkewA() {
    // Constructs two tables. The schema of each record is
    // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         2
        //    "left" |         3 |         3
        //    "left" |         4 |         4
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //   "right" |         1 |         3
        //   "right" |         2 |         4
        //   "right" |         3 |         5
        //   "right" |         4 |         6
        //
        // We expect the following matches to occur
        //
        //         +---------+
        //       4 |   x     |
        // Left  3 | x       |
        //       2 |         |
        //       1 |         |
        //         +---------+
        //           3 4 5 6
        //            Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,2,3,4};
            int[] rightValues = {3,4,5,6};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test4x4SkewB() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         3
        //    "left" |         2 |         4
        //    "left" |         3 |         5
        //    "left" |         4 |         6
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         2
        //   "right" |         3 |         3
        //   "right" |         4 |         4
        //
        // We expect the following matches to occur
        //
        //         +---------+
        //       6 |         |
        // Left  5 |         |
        //       4 |       x |
        //       3 |     x   |
        //         +---------+
        //           1 2 3 4
        //            Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {3,4,5,6};
            int[] rightValues = {1,2,3,4};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test4x4NoMatch() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         2
        //    "left" |         3 |         3
        //    "left" |         4 |         4
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //   "right" |         1 |         5
        //   "right" |         2 |         6
        //   "right" |         3 |         7
        //   "right" |         4 |         8
        //
        // We expect the following matches to occur
        //
        //         +---------+
        //       4 |         |
        // Left  3 |         |
        //       2 |         |
        //       1 |         |
        //         +---------+
        //           5 6 7 8
        //            Right
        // (x's represent where we expect to see matches)
        // (There are no matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,2,3,4};
            int[] rightValues = {5,6,7,8};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test4x4Full() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         1
        //    "left" |         3 |         1
        //    "left" |         4 |         1
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         1
        //   "right" |         3 |         1
        //   "right" |         4 |         1
        //
        // We expect the following matches to occur
        //
        //         +---------+
        //       1 | x x x x |
        // Left  1 | x x x x |
        //       1 | x x x x |
        //       1 | x x x x |
        //         +---------+
        //           1 1 1 1
        //            Right
        // (x's represent where we expect to see matches)
        // (There are no matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,1,1,1};
            int[] rightValues = {1,1,1,1};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test4x4Gaps() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         2
        //    "left" |         3 |         3
        //    "left" |         4 |         4
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //   "right" |         1 |         2
        //   "right" |         2 |         4
        //   "right" |         3 |         6
        //   "right" |         4 |         8
        //
        // We expect the following matches to occur
        //
        //         +---------+
        //       4 |   x     |
        // Left  3 |         |
        //       2 | x       |
        //       1 |         |
        //         +---------+
        //           2 4 6 8
        //            Right
        // (x's represent where we expect to see matches)
        // (There are no matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,2,3,4};
            int[] rightValues = {2,4,6,8};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test8x8() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         2
        //    "left" |         3 |         3
        //    "left" |         4 |         4
        //    "left" |         5 |         5
        //    "left" |         6 |         6
        //    "left" |         7 |         7
        //    "left" |         8 |         8
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         2
        //   "right" |         3 |         3
        //   "right" |         4 |         4
        //   "right" |         5 |         5
        //   "right" |         6 |         6
        //   "right" |         7 |         7
        //   "right" |         8 |         8
        //
        // The tables will be joined on the last column
        //         +-----------------+
        //       8 |               x |
        //       7 |             x   |
        //       6 |           x     |
        //  Left 5 |         x       |
        //       4 |       x         |
        //       3 |     x           |
        //       2 |   x             |
        //       1 | x               |
        //         +-----------------+
        //           1 2 3 4 5 6 7 8
        //                 Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,2,3,4,5,6,7,8};
            int[] rightValues = {1,2,3,4,5,6,7,8};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test8x8ClusteredA() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         1
        //    "left" |         3 |         2
        //    "left" |         4 |         2
        //    "left" |         5 |         3
        //    "left" |         6 |         3
        //    "left" |         7 |         4
        //    "left" |         8 |         4
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         1
        //   "right" |         3 |         2
        //   "right" |         4 |         2
        //   "right" |         5 |         3
        //   "right" |         6 |         3
        //   "right" |         7 |         4
        //   "right" |         8 |         4
        //
        // The tables will be joined on the last column
        //        +-----------------+
        //      4 |             x x |
        //      4 |             x x |
        //      3 |         x x     |
        // Left 3 |         x x     |
        //      2 |     x x         |
        //      2 |     x x         |
        //      1 | x x             |
        //      1 | x x             |
        //        +-----------------+
        //          1 1 2 2 3 3 4 4
        //                 Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,1,2,2,3,3,4,4};
            int[] rightValues = {1,1,2,2,3,3,4,4};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test8x8ClusteredB() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         1
        //    "left" |         3 |         1
        //    "left" |         4 |         2
        //    "left" |         5 |         3
        //    "left" |         6 |         3
        //    "left" |         7 |         3
        //    "left" |         8 |         4
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         1
        //   "right" |         3 |         1
        //   "right" |         4 |         2
        //   "right" |         5 |         3
        //   "right" |         6 |         3
        //   "right" |         7 |         3
        //   "right" |         8 |         4
        //
        // The tables will be joined on the last column
        //        +-----------------+
        //      4 |               x |
        //      3 |         x x x   |
        //      3 |         x x x   |
        // Left 3 |         x x x   |
        //      2 |       x         |
        //      1 | x x x           |
        //      1 | x x x           |
        //      1 | x x x           |
        //        +-----------------+
        //          1 1 1 2 3 3 3 4
        //                 Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,1,1,2,3,3,3,4};
            int[] rightValues = {1,1,1,2,3,3,3,4};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test8x8ClusteredC() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         1
        //    "left" |         3 |         2
        //    "left" |         4 |         2
        //    "left" |         5 |         3
        //    "left" |         6 |         3
        //    "left" |         7 |         4
        //    "left" |         8 |         4
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         2
        //   "right" |         3 |         2
        //   "right" |         4 |         2
        //   "right" |         5 |         3
        //   "right" |         6 |         3
        //   "right" |         7 |         3
        //   "right" |         8 |         4
        //
        // The tables will be joined on the last column
        //        +-----------------+
        //      4 |               x |
        //      4 |               x |
        //      3 |         x x x   |
        // Left 3 |         x x x   |
        //      2 |   x x x         |
        //      2 |   x x x         |
        //      1 | x               |
        //      1 | x               |
        //        +-----------------+
        //          1 2 2 2 3 3 3 4
        //                 Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,1,2,2,3,3,4,4};
            int[] rightValues = {1,2,2,2,3,3,3,4};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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
    public void test8x8ClusteredGaps() {
        // Constructs two tables. The schema of each record is
        // (STRING, INT INT). Here's what the left table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+-----------+----------
        //    "left" |         1 |         1
        //    "left" |         2 |         1
        //    "left" |         3 |         3
        //    "left" |         4 |         3
        //    "left" |         5 |         4
        //    "left" |         6 |         4
        //    "left" |         7 |         5
        //    "left" |         8 |         5
        //
        // and here's what the right table looks like:
        //
        // tableName | recordNum | joinValue
        // ----------+---------+-----------+----------
        //   "right" |         1 |         1
        //   "right" |         2 |         1
        //   "right" |         3 |         2
        //   "right" |         4 |         2
        //   "right" |         5 |         3
        //   "right" |         6 |         3
        //   "right" |         7 |         6
        //   "right" |         8 |         6
        //
        // The tables will be joined on the last column
        //        +-----------------+
        //      5 |                 |
        //      5 |                 |
        //      4 |         x x     |
        // Left 4 |         x x     |
        //      2 |                 |
        //      2 |                 |
        //      1 | x x             |
        //      1 | x x             |
        //        +-----------------+
        //          1 1 3 3 4 4 6 6
        //                 Right
        // (x's represent where we expect to see matches)

        try(Transaction transaction = d.beginTransaction()) {
            int[] leftValues = {1,1,2,2,4,4,5,5};
            int[] rightValues = {1,1,3,3,4,4,6,6};
            Pair<SMJVisualizer, List<Record>> p = setupValues(leftValues, rightValues);
            SMJVisualizer viz = p.getFirst();
            List<Record> expectedRecords = p.getSecond();
            QueryOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "joinValue", "joinValue",
                    transaction.getTransactionContext());
            Iterator<Record> outputIterator = joinOperator.iterator();
            for (int i = 0; outputIterator.hasNext() && i < expectedRecords.size(); i++) {
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

    public class SMJVisualizer {
        private int numLeft;
        private int numRight;
        private String grid;
        private List<Record> repeats;
        private int mismatchedNum;
        private boolean[][] actual;
        private boolean[][] expected;
        private String[][] firstMismatch;
        private String[][] fullRun;

        public SMJVisualizer(List<Record> leftRecords, List<Record> rightRecords, List<Record> expectedOutput) {
            this.numLeft = leftRecords.size();
            this.numRight = rightRecords.size();
            this.mismatchedNum = -1;
            this.repeats = new ArrayList<>();
            List<String> rows = new ArrayList<>();
            rows.add(createSeparator());
            for (int i = numLeft; i > 0; i--) {
                String prefix = leftRecords.get(i - 1).getValue(2).getInt() + " |";
                rows.add(createRow(prefix));
            }
            rows.add(createSeparator());
            rows.addAll(createRightLabels(rightRecords));
            this.grid = String.join("\n", rows) + "\n";

            this.firstMismatch = new String[numLeft][numRight];
            this.fullRun = new String[numLeft][numRight];
            for (int i = 0; i < numLeft; i++) {
                for (int j = 0; j < numRight; j++) {
                    this.fullRun[i][j] = " ";
                    this.firstMismatch[i][j] = " ";
                }
            }

            this.expected = new boolean[numLeft][numRight];
            this.actual = new boolean[numLeft][numRight];
            for (Record r: expectedOutput) {
                int leftRecord = r.getValue(1).getInt();
                int rightRecord = r.getValue(4).getInt();
                this.expected[getIndex(leftRecord)][getIndex(rightRecord)] = true;
            }
        }

        private String createSeparator() {
            StringBuilder b = new StringBuilder("  +");
            for (int i = 0; i < this.numRight; i++) {
                b.append("--");
            }
            b.append("-+");
            return b.toString();
        }

        public boolean isMismatched() {
            return this.mismatchedNum != -1;
        }

        private String createRow(String prefix) {
            StringBuilder b = new StringBuilder(prefix);
            for (int i = 0; i < this.numRight; i++) {
                b.append(" %s");
            }
            b.append(" |");
            return b.toString();
        }

        private List<String> createRightLabels(List<Record> rightRecords) {
            StringBuilder b = new StringBuilder("   ");
            for (int i = 0; i < this.numRight; i++) {
                b.append(" " + rightRecords.get(i).getValue(2).getInt());
            }
            b.append(" ");
            return Arrays.asList(b.toString());
        }

        private String visualizeState(String[][] state) {
            String[] vals = new String[this.numRight * this.numLeft];
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
                int leftIndex = getIndex(r.getValue(1).getInt());
                int rightIndex = getIndex(r.getValue(4).getInt());
                this.fullRun[leftIndex][rightIndex] = "r";
            }
            return problem;
        }

        public void add(Record expectedRecord, Record actualRecord, int num) {
            assertEquals("Your output records should have 6 values. Did you join the left and right records properly?",6, actualRecord.size());
            int expectedLeftRecord = expectedRecord.getValue(1).getInt();
            int expectedRightRecord = expectedRecord.getValue(4).getInt();
            int actualLeftRecord = actualRecord.getValue(1).getInt();
            int actualRightRecord = actualRecord.getValue(4).getInt();

            int expectedLeftIndex = getIndex(expectedLeftRecord);
            int expectedRightIndex = getIndex(expectedRightRecord);
            int actualLeftIndex = getIndex(actualLeftRecord);
            int actualRightIndex = getIndex(actualRightRecord);

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

        private int getIndex(int recordNum) {
            return recordNum-1;
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
}
