package edu.berkeley.cs186.database.databox;

import java.nio.ByteBuffer;

public class BoolDataBox extends DataBox {
    private boolean b;

    public BoolDataBox(boolean b) {
        this.b = b;
    }

    @Override
    public Type type() { return Type.boolType(); }

    @Override
    public TypeId getTypeId() { return TypeId.BOOL; }

    @Override
    public boolean getBool() {
        return this.b;
    }

    @Override
    public byte[] toBytes() {
        byte val = b ? (byte) 1 : (byte) 0;
        return ByteBuffer.allocate(1).put(val).array();
    }

    @Override
    public String toString() {
        return Boolean.toString(b);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BoolDataBox)) {
            return false;
        }
        BoolDataBox b = (BoolDataBox) o;
        return this.b == b.b;
    }

    @Override
    public int hashCode() {
        return Boolean.valueOf(b).hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (!(d instanceof BoolDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.",
                                       toString(), d.toString());
            throw new IllegalArgumentException(err);
        }
        BoolDataBox b = (BoolDataBox) d;
        return Boolean.compare(this.b, b.b);
    }
}
