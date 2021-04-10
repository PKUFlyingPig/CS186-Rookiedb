package edu.berkeley.cs186.database.common;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterable;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.ConcatBacktrackingIterator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestBacktrackingIterator {
    public <T> void testBacktrackingIterator(BacktrackingIterator<T> iter, T[] expectedValues, Random gen) {
        if (expectedValues.length == 0) {
            assertFalse(iter.hasNext());
            // These won't do anything, we're just making sure nothing errors
            iter.markNext();
            iter.markPrev();
            iter.reset();
            assertFalse(iter.hasNext());
            return;
        }
        iter.markNext();
        int start = 0;
        while (start < expectedValues.length) {
            for (int i = start; i < expectedValues.length; i++) {
                assertTrue(iter.hasNext());
                T n = iter.next();
                assertEquals("Mismatch at " + i, i, n);
            }
            assertFalse(iter.hasNext());
            int advance = Integer.min(expectedValues.length, start + gen.nextInt(5));
            boolean prev = gen.nextBoolean();
            iter.reset();
            for (int i = start; i < advance; i++) {
                assertTrue(iter.hasNext());
                T n = iter.next();
                assertEquals("Mismatch at " + i, i, n);
            }
            if (prev) {
                iter.markPrev();
                // markPrev does nothing if you haven't called next since the last reset
                if (advance == start) continue;
                start = advance - 1;
                iter.reset();
            } else {
                iter.markNext();
                start = advance;
            }
        }
        assertFalse(iter.hasNext());
    }

    @Test
    public void testArrayBacktrackingIterator() {
        Integer[] arr = new Integer[186];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = i;
        }
        Random gen = new Random(186);
        for (int r = 0; r < 20; r++) {
            ArrayBacktrackingIterator<Integer> iter = new ArrayBacktrackingIterator<>(arr);
            testBacktrackingIterator(iter, arr, gen);
        }
        arr = new Integer[0];
        testBacktrackingIterator(new ArrayBacktrackingIterator<>(arr), arr, gen);
    }

    @Test
    public void testConcatBacktrackingIterator() {
        Integer[] arr = new Integer[186];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = i;
        }
        Random gen = new Random(186);
        for (int r = 0; r < 20; r++) {
            int start = 0;
            ArrayList<BacktrackingIterable> iterables = new ArrayList<>();
            while (start < arr.length) {
                int runLen = gen.nextInt(Integer.min(10, arr.length - start)) + 1;
                if (gen.nextBoolean()) {
                    Integer[] run = new Integer[runLen];
                    for (int i = 0; i < runLen; i++) {
                        run[i] = start + i;
                    }
                    iterables.add(new IntegerArrayBackTrackingIterable(run));
                    start +=  runLen;
                } else {
                    // Toss in a few empty iterables
                    iterables.add(new IntegerArrayBackTrackingIterable(new Integer[0]));
                }
            }
            IntegerArrayBackTrackingIterable[] iterablesArr = new IntegerArrayBackTrackingIterable[iterables.size()];
            iterables.toArray(iterablesArr);
            ConcatBacktrackingIterator<Integer> iter = new ConcatBacktrackingIterator<>(
                    new ArrayBacktrackingIterator<>(iterablesArr)
            );
            testBacktrackingIterator(iter, arr, gen);
        }
        // Empty tests
        testBacktrackingIterator(
                new ArrayBacktrackingIterator<>(new BacktrackingIterable[0]),
                new BacktrackingIterable[0],
                gen
        );
    }

    private class IntegerArrayBackTrackingIterable implements BacktrackingIterable<Integer> {
        private Integer[] arr;

        public IntegerArrayBackTrackingIterable(Integer[] arr) {
            this.arr = arr;
        }

        @Override
        public BacktrackingIterator<Integer> iterator() {
            return new ArrayBacktrackingIterator<>(arr);
        }
    }
}
