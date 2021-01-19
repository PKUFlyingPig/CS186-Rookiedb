package edu.berkeley.cs186.database.common.iterator;

import java.util.NoSuchElementException;

/**
 * Partial implementation of a backtracking iterator over a collection that supports
 * indexing, with some indices possibly being "empty" and not matching to a value.
 * Subclasses only need to implement getNextNonEmpty(int) and getValue(int), the
 * rest of the mark and reset logic is handled in this class.
 */
public abstract class IndexBacktrackingIterator<T> implements BacktrackingIterator<T> {
    private int maxIndex; // highest index of the collection
    private int prevIndex = -1; // index of the previously yielded item
    private int nextIndex = -1; // index of the next item to be yielded
    private int markIndex = -1; // index of the most recently marked item

    public IndexBacktrackingIterator(int maxIndex) {
        this.maxIndex = maxIndex;
    }

    /**
     * Get the next non-empty index. Initial call uses -1.
     * @return next non-empty index or the max index if no more values.
     */
    protected abstract int getNextNonEmpty(int currentIndex);

    /**
     * Get the value at the given index. Index will always be a value returned
     * by getNextNonEmpty.
     * @param index index to get value at
     * @return value at index
     */
    protected abstract T getValue(int index);

    @Override
    public boolean hasNext() {
        if (this.nextIndex == -1) this.nextIndex = getNextNonEmpty(nextIndex);
        return this.nextIndex < this.maxIndex;
    }

    @Override
    public T next() {
        if (!this.hasNext()) throw new NoSuchElementException();
        T value = getValue(this.nextIndex);
        this.prevIndex = this.nextIndex;
        this.nextIndex = this.getNextNonEmpty(this.nextIndex);
        return value;
    }

    @Override
    public void markPrev() {
        if (prevIndex == -1) return;
        this.markIndex = this.prevIndex;
    }

    @Override
    public void markNext() {
        if (hasNext()) markIndex = nextIndex;
    }

    @Override
    public void reset() {
        if (this.markIndex == -1) return;
        this.prevIndex = -1;
        this.nextIndex = this.markIndex;
    }
}
