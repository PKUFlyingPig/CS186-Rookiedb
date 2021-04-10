package edu.berkeley.cs186.database.databox;
import java.nio.ByteBuffer;

public class LongDataBox extends DataBox {
    private long l;

    public LongDataBox(long l) {
        this.l = l;
    }

    @Override
    public Type type() {
        return Type.longType();
    }

    @Override
    public TypeId getTypeId() { return TypeId.LONG; }

    @Override
    public long getLong() {
        return this.l;
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).array();
    }

    @Override
    public String toString() {
        return Long.toString(l);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LongDataBox)) {
            return false;
        }
        LongDataBox l = (LongDataBox) o;
        return this.l == l.l;
    }

    @Override
    public int hashCode() {
        return new Long(l).hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (d instanceof FloatDataBox) {
            float f = d.getFloat();
            if (l == f) return 0;
            return l > f ? 1 : -1;
        }
        if (d instanceof IntDataBox) {
            int i = d.getInt();
            if (l == i) return 0;
            return l > i ? 1 : -1;
        }
        if (!(d instanceof LongDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.",
                                       toString(), d.toString());
            throw new IllegalArgumentException(err);
        }
        LongDataBox l = (LongDataBox) d;
        return Long.compare(this.l, l.l);
    }
}
