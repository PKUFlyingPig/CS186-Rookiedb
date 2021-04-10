package edu.berkeley.cs186.database.databox;

public class ByteArrayDataBox extends DataBox {
    byte[] bytes;

    public ByteArrayDataBox(byte[] bytes, int n) {
        if (bytes.length != n) {
            throw new RuntimeException("n must be equal to the length of bytes");
        }
        this.bytes = bytes;
    }

    @Override
    public Type type() {
        return Type.byteArrayType(bytes.length);
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.BYTE_ARRAY;
    }

    @Override
    public byte[] toBytes() {
        return this.bytes;
    }

    @Override
    public int compareTo(DataBox other) {
        throw new RuntimeException("Cannot compare byte arrays");
    }
    @Override
    public String toString() {
        return "byte_array";
    }
}
