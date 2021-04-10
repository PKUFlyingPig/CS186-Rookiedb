package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.TypeId;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Table;

import java.util.Iterator;

/**
 * A histogram maintains approximate statistics about a (potentially large) set
 * of values without explicitly storing the values. For example, given the
 * following set of numbers:
 *
 *   4, 9, 10, 10, 10, 13, 15, 16, 18, 18, 22, 23, 25, 26, 24, 42, 42, 42
 *
 * We can build the following histogram:
 *
 *   10 |
 *    9 |         8
 *    8 |       +----+
 *    7 |       |    |
 *    6 |       |    |
 *    5 |       |    | 4
 *    4 |       |    +----+      3
 *    3 |    2  |    |    |    +----+
 *    2 |  +----+    |    | 1  |    |
 *    1 |  |    |    |    +----+    |
 *    0 |  |    |    |    |    |    |
 *       ------------------------------
 *        0    10    20   30   40    50
 *
 * A histogram is an ordered list of B "buckets", each of which defines a range (low, high).
 * For the first, B - 1 buckets, the low of the range is inclusive an the high of the
 * range is exclusive. For the last Bucket the high of the range is inclusive as well.
 * Each bucket counts the number of values that fall within its range. In this project,
 * you will work with a floating point histogram where low and high are defined by floats.
 * For any other data type, we will map it so it fits into a floating point histogram.
 *
 *
 * The primary data structure to consider is Bucket<Float>[] buckets, which is a list of Bucket
 * objects
 *
 * Bucket<Float> b = new Bucket(10.0, 100.0); //defines a bucket whose low value is 10 and high is 100
 * b.getStart(); //returns 10.0
 * b.getEnd(); //returns 100.0
 * b.increment(15);// adds the value 15 to the bucket
 * b.getCount();//returns the number of items added to the bucket
 * b.getDistinctCount();//returns the approximate number of distinct items added to the bucket
 *
 *
 */
public class Histogram {
    private Bucket[] buckets; //An array of float buckets the basic data structure

    private float minValue;
    private float maxValue;
    private float width;    // The width of each bucket

    /*This constructor initialize an empty histogram object*/
    public Histogram() {
        this(1);
    }

    /*This constructor initialize a histogram object with a set number of buckets*/
    public Histogram(int numBuckets) {
        buckets = new Bucket[numBuckets];
        for (int i = 0; i < numBuckets; ++i) {
            buckets[i] = new Bucket(Float.MIN_VALUE, Float.MAX_VALUE);
        }
    }

    /*This is a copy constructor that generates a new histogram from a bucket list*/
    private Histogram(Bucket[] buckets) {
        this.buckets = buckets;
        this.minValue = buckets[0].getStart();
        this.width = buckets[0].getEnd() - buckets[0].getStart();
        this.maxValue = buckets[this.buckets.length - 1].getEnd();
    }

    /** We only consider float histograms, and these two methods turn every data type into a float.
     *  We call this mapping quantization. That means given any DataBox, we turn it into a float number.
     *  For Booleans, Integers, Floats, order is preserved in the mapping. But for strings, only equalities
     *  are preserved.
     */
    private float quantization(Record record, int index) {
        DataBox d = record.getValue(index);
        return quantization(d);
    }

    private float quantization(DataBox d) {
        switch (d.getTypeId()) {
        case BOOL:   { return (d.getBool()) ? 1.0f : 0.0f; }
        case INT:    { return (float) d.getInt(); }
        case FLOAT:  { return d.getFloat(); }
        case LONG:   { return (float) d.getLong(); }
        case STRING: { return (float) (d.getString().hashCode()); }
        }
        throw new IllegalStateException("Unreachable code.");
    }

    /** buildHistogram() takes a table and an attribute and builds a fixed width histogram, with
     *  the following procedure.
     *
     *  1. Take a pass through the full table, and store the min and the max "quantized" value.
     *  2. Calculate the width which is the (max - min)/#buckets
     *  3. Create empty bucket objects and place them in the array.
     *  4. Populate the buckets by incrementing
     *
     *  Edge cases: width = 0, put an item only in the last bucket.
     *              final buckunreachableet is inclusive on the last value.
     */
    public void buildHistogram(Table table, int attribute) {
        // 1. first calculate the min and the max values
        // 2. calculate the width of each bin
        // 3. create each bucket object
        // 4. populate the data using the increment(value) method
        Iterator<Record> iter = table.iterator();
        while (iter.hasNext()) {
            Record record = iter.next();
            float quantizedValue = quantization(record, attribute);
            this.minValue = Math.min(this.minValue, quantizedValue);
            this.maxValue = Math.max(this.maxValue, quantizedValue);
        }

        this.width = (this.maxValue - this.minValue) / this.buckets.length;

        for (int i = 0; i < this.buckets.length; i++) {
            buckets[i] = new Bucket(this.minValue + (i) * width, this.minValue + (i + 1) * width);
        }

        iter = table.iterator();
        while (iter.hasNext()) {
            Record record = iter.next();
            float quantizedValue = quantization(record, attribute);

            int bucketIndex;

            if (this.width == 0) {
                bucketIndex = this.buckets.length - 1; //always put in the last bin
            } else {
                bucketIndex = (int) Math.floor((quantizedValue - this.minValue) / this.width);
                bucketIndex = Math.max(0, bucketIndex);
                bucketIndex = Math.min(bucketIndex, this.buckets.length - 1);
            }

            buckets[bucketIndex].increment(quantizedValue);
        }
    }
    //Accessor Methods//////////////////////////////////////////////////////////////
    /** Return an estimate of the number of distinct values in the histogram. */
    public int getNumDistinct() {
        int sum = 0;
        for (Bucket bucket : this.buckets) sum += bucket.getDistinctCount();
        return sum;
    }

    /** Return an estimate of the number of the total values in the histogram. */
    public int getCount() {
        int sum = 0;
        for (Bucket bucket : this.buckets) sum += bucket.getCount();
        return sum;
    }

    /* Returns the bucket object at i */
    public Bucket get(int i) {
        return buckets[i];
    }

    //Operations//////////////////////////////////////////////////////////////

    /* Given a predicate, return a multiplicative mask for the histogram. That is,
     * an array of size numBuckets where each entry is a float between 0 and 1 that represents a
     * scaling to update the histogram count. Suppose we have this histogram with 5 buckets:
     *
     *   10 |
     *    9 |         8
     *    8 |       +----+
     *    7 |       |    |
     *    6 |       |    |
     *    5 |       |    | 4
     *    4 |       |    +----+      3
     *    3 |    2  |    |    |    +----+
     *    2 |  +----+    |    | 1  |    |
     *    1 |  |    |    |    +----+    |
     *    0 |  |    |    |    |    |    |
     *       ------------------------------
     *        0    10    20   30   40    50
     *
     * Then we get a mask, [0, .25, .5, 0, 1], the resulting histogram is:
     *
     *   10 |
     *    9 |
     *    8 |
     *    7 |
     *    6 |
     *    5 |
     *    4 |                        3
     *    3 |          2    2      +----+
     *    2 |       +----+----+    |    |
     *    1 |       |    |    |    |    |
     *    0 |    0  |    |    |  0 |    |
     *       ------------------------------
     *        0    10    20   30   40    50
     *
     * Counts are always an integer and round to the nearest value.
     */
    public float[] filter(PredicateOperator predicate, DataBox value) {
        float quant =  quantization(value);

        //do not handle non equality predicates on strings
        if (value.getTypeId() == TypeId.STRING &&
                predicate != PredicateOperator.EQUALS &&
                predicate != PredicateOperator.NOT_EQUALS) {
            return stringNonEquality(quant);
        } else if (predicate == PredicateOperator.EQUALS) {
            return allEquality(quant);
        } else if (predicate == PredicateOperator.NOT_EQUALS) {
            return allNotEquality(quant);
        } else if (predicate == PredicateOperator.GREATER_THAN) {
            return allGreaterThan(quant);
        } else if (predicate == PredicateOperator.LESS_THAN) {
            return allLessThan(quant);
        } else if (predicate == PredicateOperator.GREATER_THAN_EQUALS) {
            return allGreaterThanEquals(quant);
        } else {
            return allLessThanEquals(quant);
        }
    }

    /** Given, we don't handle non equality comparisons of strings. Return 1*/
    private float[] stringNonEquality(float qvalue) {
        float[] result = new float[this.buckets.length];
        for (int i = 0; i < this.buckets.length; i++) {
            result[i] = 1.0f;
        }

        return result;
    }

    /*Nothing fancy here take max of gt and equals*/
    private float[] allGreaterThanEquals(float qvalue) {
        float[] result = new float[this.buckets.length];
        float[] resultGT = allGreaterThan(qvalue);
        float[] resultEquals = allEquality(qvalue);

        for (int i = 0; i < this.buckets.length; i++) {
            result[i] = Math.max(resultGT[i], resultEquals[i]);
        }

        return result;
    }

    /*Nothing fancy here take max of lt and equals*/
    private float[] allLessThanEquals(float qvalue) {
        float[] result = new float[this.buckets.length];
        float[] resultLT = allLessThan(qvalue);
        float[] resultEquals = allEquality(qvalue);

        for (int i = 0; i < this.buckets.length; i++) {
            result[i] = Math.max(resultLT[i], resultEquals[i]);
        }

        return result;
    }

    //See comments above filter()

    /**
     *  Given a quantized value, set the bucket that contains the value to 1/distinctCount,
     *  and set all other values to 0.
     */
    private float[] allEquality(float qvalue) {
        float[] result = new float[this.buckets.length];

        for (int i = 0; i < this.buckets.length - 1; i++) {
            if (qvalue >= this.buckets[i].getStart() &&
                    qvalue < this.buckets[i].getEnd()) {
                result[i] = Math.min(1.0f / this.buckets[i].getDistinctCount(), 1.0f);
            } else {
                result[i] = 0.0f;
            }
        }

        //handle last value
        if (qvalue >= this.buckets[this.buckets.length - 1].getStart() &&
                qvalue <= this.buckets[this.buckets.length - 1].getEnd()) {
            int distinctCount = this.buckets[this.buckets.length - 1].getDistinctCount();
            result[this.buckets.length - 1] = Math.min(1.0f / distinctCount, 1.0f);
        } else {
            result[this.buckets.length - 1] = 0.0f;
        }

        return result;
    }

    /**
      *  Given a quantized value, set the bucket that contains the value by 1-1/distinctCount,
      *  and set all other values to 1.
      */
    private float[] allNotEquality(float qvalue) {
        float[] result = new float[this.buckets.length];

        for (int i = 0; i < this.buckets.length - 1; i++) {
            if (qvalue >= this.buckets[i].getStart() &&
                    qvalue < this.buckets[i].getEnd()) {
                result[i] = Math.max(1.0f - 1.0f / this.buckets[i].getDistinctCount(), 0.0f);
            } else {
                result[i] = 1.0f;
            }
        }
        //handle last value
        if (qvalue >= this.buckets[this.buckets.length - 1].getStart() &&
                qvalue <= this.buckets[this.buckets.length - 1].getEnd()) {
            int distinctCount = this.buckets[this.buckets.length - 1].getDistinctCount();
            result[this.buckets.length - 1] = Math.max(1 - 1.0f / distinctCount, 0.0f);
        } else {
            result[this.buckets.length - 1] = 1.0f;
        }

        return result;
    }

    /**
     *  Given a quantized value, set the bucket that contains the value by (end - q)/width,
     *  and set all other buckets to 1 if higher and 0 if lower.
     */
    private float[] allGreaterThan(float qvalue) {
        float[] result = new float[this.buckets.length];

        for (int i = 0; i < this.buckets.length; i++) {
            if (qvalue >= this.buckets[i].getStart() && qvalue < this.buckets[i].getEnd()) {
                result[i] = (this.buckets[i].getEnd() - qvalue) / this.width;
            } else if (qvalue < this.buckets[i].getStart()) {
                result[i] = 1.0f;
            } else {
                result[i] = 0.0f;
            }
        }

        return result;
    }

    /**
      *  Given a quantized value, set the bucket that contains the value by (q-start)/width,
      *  and set all other buckets to 1 if lower and 0 if higher.
      */
    private float[] allLessThan(float qvalue) {
        float[] result = new float[this.buckets.length];

        for (int i = 0; i < this.buckets.length; i++) {
            if (qvalue >= this.buckets[i].getStart() && qvalue < this.buckets[i].getEnd()) {
                result[i] = (qvalue - this.buckets[i].getStart()) / this.width;
            } else if (qvalue >= this.buckets[i].getEnd()) {
                result[i] = 1.0f;
            } else {
                result[i] = 0.0f;
            }
        }

        return result;
    }

    // Cost Estimation ///////////////////////////////////////////////////////////////////

    /**
     * Return an estimate of the reduction factor for a given filter. For
     * example, consider again the example histogram from the top of the file.
     * The reduction factor for the predicate `>= 25` is 0.5 because roughly half
     * of the values are greater than or equal to 25.
     */
    public float computeReductionFactor(PredicateOperator predicate, DataBox value) {
        float[] reduction = filter(predicate, value);

        float sum = 0.0f;
        int total = 0;

        for (int i = 0; i < this.buckets.length; i++) {
            //non empty buckets
            sum += reduction[i] * this.buckets[i].getDistinctCount();
            total += this.buckets[i].getDistinctCount();
        }

        return sum / total;
    }

    /**
     * Given a histogram for a dataset, return a new histogram for the same
     * dataset with a filter applied. For example, if apply the filter `>= 20` to
     * the example histogram from the top of the file, we would get the following
     * histogram:
     *
     *    6 |
     *    5 |              4
     *    4 |            +----+      3
     *    3 |            |    |    +----+
     *    2 |            |    | 1  |    |
     *    1 |    0    0  |    +----+    |
     *    0 |  +----+----+    |    |    |
     *       ------------------------------
     *         0    10   20   30   40   50]
     */
    public Histogram copyWithPredicate(PredicateOperator predicate, DataBox value) {
        float[] reduction = filter(predicate, value);
        Bucket[] newBuckets = new Bucket[this.buckets.length];

        for (int i = 0; i < this.buckets.length; i++) {
            newBuckets[i] = new Bucket(this.buckets[i]);
            int newCount = Math.round(reduction[i] * this.buckets[i].getCount());
            int newDistinctCount = Math.round(reduction[i] * this.buckets[i].getDistinctCount());
            newBuckets[i].setCount(newCount);
            newBuckets[i].setDistinctCount(newDistinctCount);
        }

        Histogram h = new Histogram(newBuckets);
        return h;
    }

    // Uniformly reduces the values across the board with the mean reduction.
    // Assumes uncorrelated
    public Histogram copyWithReduction(float reduction) {
        Bucket[] newBuckets = new Bucket[this.buckets.length];
        for (int i = 0; i < this.buckets.length; i++) {
            newBuckets[i] = new Bucket(this.buckets[i]);
            int newCount = Math.round(reduction * this.buckets[i].getCount());
            int newDistinctCount = Math.round(reduction * this.buckets[i].getDistinctCount());
            newBuckets[i].setCount(newCount);
            newBuckets[i].setDistinctCount(newDistinctCount);
        }
        return new Histogram(newBuckets);
    }

    // Updates the count of each
    public Histogram copyWithJoin(int newTotal, float reduction) {
        Bucket[] newBuckets = new Bucket[this.buckets.length];
        for (int i = 0; i < this.buckets.length; i++) {
            newBuckets[i] = new Bucket(this.buckets[i]);
            int oldCount = this.buckets[i].getCount();
            int newCount = Math.round((float) oldCount * getCount() / newTotal);
            int newDistinctCount = Math.round(reduction * this.buckets[i].getDistinctCount());
            newBuckets[i].setCount(newCount);
            newBuckets[i].setDistinctCount(newDistinctCount);
        }
        return new Histogram(newBuckets);
    }
}
