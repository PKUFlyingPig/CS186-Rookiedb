package edu.berkeley.cs186.database.common.iterator;

public interface BacktrackingIterable<T> extends Iterable<T> {
    @Override
    BacktrackingIterator<T> iterator();
}
