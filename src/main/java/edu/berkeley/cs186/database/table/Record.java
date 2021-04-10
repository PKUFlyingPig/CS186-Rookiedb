package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.databox.TypeId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** A Record is just list of DataBoxes. */
public class Record {
    private List<DataBox> values;

    public Record(List<DataBox> values) {
        this.values = values;
    }

    public Record(Object... values) {
        this.values = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++) {
            this.values.add(DataBox.fromObject(values[i]));
        }
    }

    /**
     * A list of DataBox's representing this records fields.
     */
    public List<DataBox> getValues() {
        return new ArrayList<>(this.values);
    }

    /**
     * Returns the DataBox at the specified position of this record.
     */
    public DataBox getValue(int i) {
        return this.values.get(i);
    }

    /**
     * Serializes this Databox into a byte array based on the passed in schema.
     */
    public byte[] toBytes(Schema schema) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(schema.getSizeInBytes());
        for (DataBox value : values) {
            byteBuffer.put(value.toBytes());
        }
        return byteBuffer.array();
    }

    /**
     * Returns a new records consisting of this record's values with the other record's
     * values appended to the right of it. i.e. if record a contains [1,2,3] and record b
     * contains [4,5,6], a.concat(b) would be a record consisting of [1,2,3,4,5,6].
     */
    public Record concat(Record other) {
        List<DataBox> values = new ArrayList<>(this.values);
        values.addAll(other.getValues());
        return new Record(values);
    }

    /**
     * Takes a byte[] and decodes it into a Record. This method assumes that the
     * input byte[] represents a record that corresponds to this schema.
     *
     * @param buf the byte array to decode
     * @param schema the schema used for this record
     * @return the decoded Record
     */
    public static Record fromBytes(Buffer buf, Schema schema) {
        List<DataBox> values = new ArrayList<>();
        for (Type t : schema.getFieldTypes()) {
            values.add(DataBox.fromBytes(buf, t));
        }
        return new Record(values);
    }

    /**
     * @return the number of values in this record
     */
    public int size() {return values.size();}

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("(");
        for (int i = 0; i < values.size(); i++) {
            DataBox value = values.get(i);
            if (value.getTypeId() == TypeId.STRING) {
                builder.append("'").append(value).append("'");
            } else builder.append(value);
            if (i < values.size() - 1) builder.append(",");
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Record)) return false;
        Record r = (Record) o;
        return values.equals(r.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
