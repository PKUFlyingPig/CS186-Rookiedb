package edu.berkeley.cs186.database.table.stats;

import java.util.HashSet;
import java.util.Objects;

/**
 * A histogram bucket
 */
public class Bucket {
    private float start;
    private float end;
    private int count;
    private int distinctCount;
    private HashSet<Float> dictionary;

    public Bucket(float start, float end) {
        this.start = start;
        this.end = end;
        this.count = 0;
        this.distinctCount = 0;
        this.dictionary = new HashSet<>();
    }

    public Bucket(Bucket other) {
        this(other.start, other.end);
        this.count = other.count;
        this.distinctCount = other.distinctCount;
    }

    public float getStart() {
        return start;
    }

    public float getEnd() {
        return end;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setDistinctCount(int count) {
        this.distinctCount = count;
        dictionary = null;
    }

    public int getDistinctCount() {
        if (dictionary == null) return this.distinctCount;
        return dictionary.size();
    }

    public void increment(float val) {
        count++;
        dictionary.add(val);
    }

    @Override
    public String toString() {
        return String.format("[%s,%s):%d", start, end, count);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Bucket)) return false;
        Bucket b = (Bucket) o;
        boolean startEquals = start == b.start;
        boolean endEquals = end == b.end;
        boolean countEquals = count == b.count;
        return startEquals && endEquals && countEquals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, count);
    }
}
