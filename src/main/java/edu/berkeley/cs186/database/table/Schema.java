package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.databox.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The schema of a table includes the name and type of every one of its
 * fields. For example, the following schema:
 *
 *   Schema s = new Schema()
 *      .add("x", Type.intType())
 *      .add("y", Type.floatType());
 *
 * represents a table with an int field named "x" and a float field named "y".
 */
public class Schema {
    private List<String> fieldNames;
    private List<Type> fieldTypes;
    private short sizeInBytes;

    /**
     * Constructs an empty Schema.
     */
    public Schema() {
        this.fieldNames = new ArrayList<>();
        this.fieldTypes = new ArrayList<>();
        this.sizeInBytes = 0;
    }

    /**
     * Adds a new field to the schema. Returns the schema so that calls can be
     * chained together (see example above).
     * @param fieldName the name of the new field
     * @param fieldType the type of the new field
     * @return the schema that the field was added to
     */
    public Schema add(String fieldName, Type fieldType) {
        this.fieldNames.add(fieldName);
        this.fieldTypes.add(fieldType);
        this.sizeInBytes += fieldType.getSizeInBytes();
        return this;
    }

    /**
     * @return the names of the fields of this schema, in order
     */
    public List<String> getFieldNames() {
        return fieldNames;
    }

    /**
     * @return the types of the fields in this schema, in order
     */
    public List<Type> getFieldTypes() {
        return fieldTypes;
    }

    /**
     * @param i
     * @return the name of the field at the index `i`
     */
    public String getFieldName(int i) { return fieldNames.get(i); }

    /**
     * @param i
     * @return the type of the field at the index `i`
     */
    public Type getFieldType(int i) { return fieldTypes.get(i); }

    /**
     * @return the number of fields in this schema
     */
    public int size() { return this.fieldNames.size(); }

    /**
     * @return the size of this schema in bytes after being serialized
     */
    public short getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * @param fromSchema
     * @param specified
     * @return returns true if the two names can be considered equal, false
     * otherwise. Two field names are equal if they are the same ignoring case,
     * or if `specified` is unqualified and matches the unqualified portion of
     * `fromSchema`. For example "table1.someCol" and "someCol" would be
     * considered equal.
     */
    private static boolean fieldNamesEqual(String fromSchema, String specified) {
        fromSchema = fromSchema.toLowerCase();
        specified = specified.toLowerCase();
        if (fromSchema.equals(specified)) {
            return true;
        }
        if (!specified.contains(".")) {
            // specified is unqualified, remove qualification from fromSchema
            String schemaColName = fromSchema;
            if (fromSchema.contains(".")) {
                String[] splits = fromSchema.split("\\.");
                schemaColName = splits[1];
            }

            return schemaColName.equals(specified);
        }
        return false;
    }

    /**
     * @param fieldName
     * @throws RuntimeException if no field found, or ambiguous field name
     * @return finds the index of the field corresponding to fieldName
     */
    public int findField(String fieldName) {
        int index = -1;
        for (int i = 0; i < this.size(); i++) {
            String fromSchema = this.fieldNames.get(i);
            if (fieldNamesEqual(fromSchema, fieldName)) {
                if (index != -1) {
                    throw new RuntimeException("Column " + fieldName + " specified twice without disambiguation in " + toString());
                } else index = i;
            }
        }
        if (index == -1) throw new RuntimeException("No column " + fieldName + " found in " + toString());
        return index;
    }

    /**
     * @param fieldName
     * @return the name of the provided field as it appears in this schema,
     * matching case and qualification
     */
    public String matchFieldName(String fieldName) {
        return this.fieldNames.get(this.findField(fieldName));
    }

    /**
     * @param other
     * @return Concatenates two schema together, returning a new schema
     * containing the the fields of this schema immediately followed by the
     * fields of `other`
     */
    public Schema concat(Schema other) {
        Schema copy = new Schema();
        copy.fieldTypes = new ArrayList<>(fieldTypes);
        copy.fieldNames = new ArrayList<>(fieldNames);
        copy.sizeInBytes = sizeInBytes;
        for(int i = 0; i < other.size(); i++)
            copy.add(other.fieldNames.get(i), other.fieldTypes.get(i));
        return copy;
    }

    /**
     * Verifies that a record matches the given schema. Performs the following
     * implicit casts:
     * - String's of the wrong size are cast to the expected size of the schema
     * - Int's will be cast to floats if a float is expected
     * @param record
     * @throws DatabaseException if a field of the record does not match the
     * type of the corresponding field in the schema, and cannot be implicitly
     * cast to the correct field
     * @return A new record with fields cast to match the schema
     */
    public Record verify(Record record) {
        List<DataBox> values = record.getValues();
        if (values.size() != fieldNames.size()) {
            String err = String.format("Expected %d values, but got %d.",
                                       fieldNames.size(), values.size());
            throw new DatabaseException(err);
        }

        for (int i = 0; i < values.size(); ++i) {
            Type actual = values.get(i).type();
            Type expected = fieldTypes.get(i);
            if (!actual.equals(expected)) {
                if(actual.getTypeId() == TypeId.STRING && expected.getTypeId() == TypeId.STRING) {
                    // Implicit cast
                    DataBox wrongSize = values.get(i);
                    values.set(i, new StringDataBox(wrongSize.getString(), expected.getSizeInBytes()));
                    continue;
                }
                if(actual.getTypeId() == TypeId.INT && expected.getTypeId() == TypeId.FLOAT) {
                    // Implicit cast
                    DataBox intBox = values.get(i);
                    values.set(i, new FloatDataBox((float) intBox.getInt()));
                    continue;
                }
                String err = String.format(
                                 "Expected field %d to be of type %s, but got value of type %s.",
                                 i, expected, actual);
                throw new DatabaseException(err);
            }
        }

        return new Record(values);
    }

    /**
     * @return a byte array containing the serialized copy of the schema
     */
    public byte[] toBytes() {
        // A schema is serialized as follows. We first write the number of fields
        // (4 bytes). Then, for each field, we write
        //
        //   1. the length of the field name (4 bytes),
        //   2. the field's name,
        //   3. and the field's type.

        // First, we compute the number of bytes we need to serialize the schema.
        int size = Integer.BYTES; // The length of the schema.
        for (int i = 0; i < fieldNames.size(); ++i) {
            size += Integer.BYTES; // The length of the field name.
            size += fieldNames.get(i).length(); // The field name.
            size += fieldTypes.get(i).toBytes().length; // The type.
        }

        // Then we serialize it.
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); ++i) {
            buf.putInt(fieldNames.get(i).length());
            buf.put(fieldNames.get(i).getBytes(Charset.forName("UTF-8")));
            buf.put(fieldTypes.get(i).toBytes());
        }
        return buf.array();
    }

    /**
     * Deserializes a bytes from a buffer to create a Schema object. After this
     * function exits the next call to get() on the buffer will be the first
     * byte that wasn't part of the schema.
     *
     * @param buf a buffer to draw bytes from.
     * @return A Schema object made by deserializing the bytes from the given
     * buffer
     */
    public static Schema fromBytes(Buffer buf) {
        Schema s = new Schema();
        int size = buf.getInt();
        for (int i = 0; i < size; i++) {
            int fieldSize = buf.getInt();
            byte[] bytes = new byte[fieldSize];
            buf.get(bytes);
            s.add(new String(bytes, Charset.forName("UTF-8")), Type.fromBytes(buf));
        }
        return s;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < fieldNames.size(); ++i) {
            sb.append(String.format("%s: %s", fieldNames.get(i), fieldTypes.get(i)));
            if (i != fieldNames.size()) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Schema)) return false;
        Schema s = (Schema) o;
        return fieldNames.equals(s.fieldNames) && fieldTypes.equals(s.fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames, fieldTypes);
    }
}
