package edu.berkeley.cs186.database.databox;

import java.nio.charset.Charset;

public class StringDataBox extends DataBox {
    private String s;
    private int m;

    // Construct an m-byte string. If s has more than m-bytes, it is truncated to
    // its first m bytes. If s has fewer than m bytes, when serialized it is
    // padded with null bytes until it is exactly m bytes long.
    //
    //   - new StringDataBox("123", 5).getString()     // "123"
    //   - new StringDataBox("12345", 5).getString()   // "12345"
    //   - new StringDataBox("1234567", 5).getString() // "12345"
    public StringDataBox(String s, int m) {
        if (m <= 0) {
            String msg = String.format("Cannot construct a %d-byte string. " +
                                       "Strings must be at least one byte.", m);
            throw new IllegalArgumentException(msg);
        }
        this.m = m;
        s = m > s.length() ? s : s.substring(0, m);
        this.s = s.replaceAll("\0*$", ""); // Trim off null bytes
    }

    public StringDataBox(String s) {
        this(s, s.length());
    }

    @Override
    public Type type() {
        return Type.stringType(m);
    }

    @Override
    public TypeId getTypeId() { return TypeId.STRING; }

    @Override
    public String getString() {
        return this.s;
    }

    @Override
    public byte[] toBytes() {
        // pad with null bytes
        String padded = s + new String(new char[m - s.length()]);
        return padded.getBytes(Charset.forName("ascii"));
    }

    @Override
    public byte[] hashBytes() {
        return s.getBytes(Charset.forName("ascii"));
    }

    @Override
    public String toString() {
        return s;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StringDataBox)) return false;
        StringDataBox other = (StringDataBox) o;
        return this.s.equals(other.s);
    }

    @Override
    public int hashCode() {
        return s.hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (!(d instanceof StringDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.",
                                       toString(), d.toString());
            throw new IllegalArgumentException(err);
        }
        StringDataBox other = (StringDataBox) d;
        return this.s.compareTo(other.s);
    }
}
