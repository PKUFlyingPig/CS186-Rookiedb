package edu.berkeley.cs186.database.databox;

import edu.berkeley.cs186.database.common.Buffer;

import java.nio.charset.Charset;

/**
 * A DataBox is an element of one of the primitive types specified in Type.java.
 * You can create
 *   - booleans with new BoolDataBox(b),
 *   - integers with new IntDataBox(i),
 *   - floats with new FloatDataBox(f),
 *   - strings with new StringDataBox(s, n), and
 *   - longs with new LongDataBox(l).
 *
 * You can unwrap a data box by first pattern matching on its type and them using
 * one of getBool, getInt, getFloat, getString, and getLong:
 *
 *   DataBox d = DataBox.fromBytes(bytes);
 *   switch (d.getTypeId()) {
 *     case BOOL:   { System.out.println(d.getBool()); }
 *     case INT:    { System.out.println(d.getInt()); }
 *     case FLOAT:  { System.out.println(d.getFloat()); }
 *     case STRING: { System.out.println(d.getString()); }
 *     case LONG:   { System.out.println(d.getLong()); }
 *   }
 */
public abstract class DataBox implements Comparable<DataBox> {
    public abstract Type type();

    public abstract TypeId getTypeId();

    public boolean getBool() { throw new RuntimeException("not boolean type"); }

    public int getInt() { throw new RuntimeException("not int type"); }

    public float getFloat() { throw new RuntimeException("not float type"); }

    public String getString() { throw new RuntimeException("not String type"); }

    public long getLong() {
        throw new RuntimeException("not Long type");
    }

    public byte[] getByteArray() { throw new RuntimeException("not Byte Array type"); }

    /**
     * Databoxes are serialized as follows:
     * - BoolDataBoxes are serialized to a single byte that is 0 if the
     *   BoolDataBox is false and 1 if the Databox is true.
     * - An IntDataBox and a FloatDataBox are serialized to their 4-byte
     *   values (e.g. using ByteBuffer::putInt or ByteBuffer::putFloat).
     * - The first byte of a serialized m-byte StringDataBox is the 4-byte
     *   number m. Then come the m bytes of the string.
     *
     * Note that when DataBoxes are serialized, they do not serialize their type.
     * That is, serialized DataBoxes are not self-descriptive; you need the type
     * of a Databox in order to parse it.
     */
    public abstract byte[] toBytes();

    /**
     * Same as toBytes() for every DataBox except for StringDataBox, which needs
     * to be handled specially to preserve equality over different lengths.
     */
    public byte[] hashBytes() {
        return toBytes();
    }

    public static DataBox fromBytes(Buffer buf, Type type) {
        switch (type.getTypeId()) {
            case BOOL: {
                byte b = buf.get();
                assert (b == 0 || b == 1);
                return new BoolDataBox(b == 1);
            }
            case INT: {
                return new IntDataBox(buf.getInt());
            }
            case FLOAT: {
                return new FloatDataBox(buf.getFloat());
            }
            case STRING: {
                byte[] bytes = new byte[type.getSizeInBytes()];
                buf.get(bytes);
                String s = new String(bytes, Charset.forName("UTF-8"));
                return new StringDataBox(s, type.getSizeInBytes());
            }
            case LONG: {
                return new LongDataBox(buf.getLong());
            }
            case BYTE_ARRAY: {
                byte[] bytes = new byte[type.getSizeInBytes()];
                buf.get(bytes);
                return new ByteArrayDataBox(bytes, type.getSizeInBytes());
            }
            default: {
                String err = String.format("Unhandled TypeId %s.",
                                           type.getTypeId().toString());
                throw new IllegalArgumentException(err);
            }
        }
    }

    public static DataBox fromString(Type type, String s) {
        String raw = s;
        s = s.toLowerCase().trim();
        switch (type.getTypeId()) {
            case BOOL: return new BoolDataBox(s.equals("true"));
            case INT: return new IntDataBox(Integer.parseInt(s));
            case LONG: return new LongDataBox(Long.parseLong(s));
            case FLOAT: return new FloatDataBox(Float.parseFloat(s));
            case STRING: return new StringDataBox(raw);
            default: throw new RuntimeException("Unreachable code");
        }
    }

    /**
     * @param o some object
     * @return if the passed in object was already DataBox then we return the
     * object after a cast. Otherwise, if the object was an instance of a
     * wrapper type for one of the primitives we support, then return a DataBox
     * of the proper type wrapping the object. Useful for making the record
     * constructor and QueryPlan methods more readable.
     *
     * Examples:
     * - DataBox.fromObject(186) ==  new IntDataBox(186)
     * - DataBox.fromObject("186") == new StringDataBox("186")
     * - DataBox.fromObject(new ArrayList<>()) // Error! ArrayList isn't a
     *                                         // primitive we support
     */
    public static DataBox fromObject(Object o) {
        if (o instanceof DataBox) return (DataBox) o;
        if (o instanceof Integer) return new IntDataBox((Integer) o);
        if (o instanceof String) return new StringDataBox((String) o);
        if (o instanceof Boolean) return new BoolDataBox((Boolean) o);
        if (o instanceof Long) return new LongDataBox((Long) o);
        if (o instanceof Float) return new FloatDataBox((Float) o);
        if (o instanceof Double) {
            // implicit cast
            double d = (Double) o;
            return new FloatDataBox((float) d);
        }
        if (o instanceof byte[]) return new ByteArrayDataBox((byte[]) o, ((byte[]) o).length);
        throw new IllegalArgumentException("Object was not a supported data type");
    }
}
