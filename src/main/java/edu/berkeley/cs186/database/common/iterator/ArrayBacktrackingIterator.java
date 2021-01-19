package edu.berkeley.cs186.database.common.iterator;

import java.util.List;

/**
 * Backtracking iterator over an array.
 */
public class ArrayBacktrackingIterator<T> extends IndexBacktrackingIterator<T> {
    protected T[] array;

    public ArrayBacktrackingIterator(T[] array) {
        super(array.length);
        this.array = array;
    }

    public ArrayBacktrackingIterator(List<T> list) {
        this((T[]) list.toArray());
    }

    @Override
    protected int getNextNonEmpty(int currentIndex) {
        return currentIndex + 1;
    }

    @Override
    protected T getValue(int index) {
        return this.array[index];
    }
}

