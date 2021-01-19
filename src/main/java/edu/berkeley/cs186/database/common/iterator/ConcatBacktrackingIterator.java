package edu.berkeley.cs186.database.common.iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator that concatenates a group of backtracking iterables together.
 * For example, if you had backtracking iterators containing the following:
 * - [1,2,3]
 * - []
 * - [4,5,6]
 * - [7,8]
 *
 * Concatenating them with this class would produce a backtracking iterator
 * over the values [1,2,3,4,5,6,7,8].
 */
public class ConcatBacktrackingIterator<T> implements BacktrackingIterator<T> {
    // Iterator of iterables that we're concatenating
    private BacktrackingIterator<BacktrackingIterable<T>> outerIterator;
    // List of iterables we're concatenating
    private List<BacktrackingIterable<T>> iterables;
    // The iterator that we yielded the previous item from
    private BacktrackingIterator<T> prevItemIterator;
    // The iterator we'll be yielding the next item from
    private BacktrackingIterator<T> nextItemIterator;
    // The iterator with the item that's currently marked
    private BacktrackingIterator<T> markItemIterator;
    // Indices of the above three iterator's source iterables
    private int prevIndex = -1;
    private int nextIndex = -1;
    private int markIndex = -1;

    /**
     * @param outerIterator An iterator over iterable objects which we want to concatenate.
     *                      Any values this iterator yields will be drawn from iterators created
     *                      from outerIterator's contents.
     */
    public ConcatBacktrackingIterator(BacktrackingIterator<BacktrackingIterable<T>> outerIterator) {
        this.iterables = new ArrayList<>();
        this.outerIterator = outerIterator;
        this.prevItemIterator = null;
        this.nextItemIterator = new EmptyBacktrackingIterator<>();
        this.markItemIterator = null;
    }

    /**
     * Sets nextItemIterator to the next non-empty iterator in our collection, or the last one if all remaining
     * iterators are empty. Lazily adds in the iterables from outerIterator as needed to the list of iterables.
     */
    private void moveNextToNonEmpty() {
        while (!this.nextItemIterator.hasNext()) {
            if (nextIndex + 1 < iterables.size()) {
                nextIndex++;
                this.nextItemIterator = iterables.get(nextIndex).iterator();
            } else {
                assert(nextIndex + 1 == iterables.size());
                if (!outerIterator.hasNext()) break;
                iterables.add(outerIterator.next());
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (!this.nextItemIterator.hasNext()) this.moveNextToNonEmpty();
        return this.nextItemIterator.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T item = this.nextItemIterator.next();
        this.prevItemIterator = this.nextItemIterator;
        prevIndex = nextIndex;
        return item;
    }

    @Override
    public void markPrev() {
        if (prevIndex == -1) {
            // We haven't yielded an item yet, or since the most recent reset
            return;
        }
        this.markItemIterator = this.prevItemIterator;
        this.markItemIterator.markPrev();
        markIndex = prevIndex;
    }

    @Override
    public void markNext() {
        if (!hasNext()) return;
        this.markItemIterator = this.nextItemIterator;
        this.markItemIterator.markNext();
        markIndex = nextIndex;
    }

    @Override
    public void reset() {
        if (markIndex == -1) {
            // Nothing was marked
            return;
        }
        // We no longer have a prev
        prevItemIterator = null;
        prevIndex = -1;

        // Set our next to wherever was marked
        this.nextItemIterator = this.markItemIterator;
        this.nextItemIterator.reset();
        nextIndex = markIndex;
    }
}