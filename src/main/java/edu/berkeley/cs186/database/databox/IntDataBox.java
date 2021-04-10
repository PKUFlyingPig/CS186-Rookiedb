package edu.berkeley.cs186.database.databox;
import java.nio.ByteBuffer;

public class IntDataBox extends DataBox {
    private int i;

    public IntDataBox(int i) {
        this.i = i;
    }

    @Override
    public Type type() {
        return Type.intType();
    }

    @Override
    public TypeId getTypeId() { return TypeId.INT; }

    @Override
    public int getInt() {
        return this.i;
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }

    @Override
    public String toString() {
        return Integer.toString(i);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof IntDataBox)) {
            return false;
        }
        IntDataBox i = (IntDataBox) o;
        return this.i == i.i;
    }

    @Override
    public int hashCode() {
        return new Integer(i).hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (d instanceof LongDataBox) {
            long l = d.getLong();
            if (i == l) return 0;
            return i > l ? 1 : -1;
        }
        if (d instanceof FloatDataBox) {
            float f = d.getFloat();
            if (i == f) return 0;
            return i > f ? 1 : -1;
        }
        if (!(d instanceof IntDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.",
                                       toString(), d.toString());
            throw new IllegalArgumentException(err);
        }
        IntDataBox i = (IntDataBox) d;
        return Integer.compare(this.i, i.i);
    }
}
