package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.databox.Type;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category({Proj99Tests.class, SystemTests.class})
public class TestRecord {
    @Test
    public void testToAndFromBytes() {
        Schema[] schemas = {
            new Schema().add("x", Type.boolType()),
            new Schema().add("x",Type.intType()),
            new Schema().add("x", Type.floatType()),
            new Schema().add("x", Type.stringType(3)),
            new Schema()
                .add("w", Type.boolType())
                .add("x", Type.intType())
                .add("y", Type.floatType())
                .add("z", Type.stringType(3))
        };

        Record[] records = {
            new Record(false),
            new Record(0),
            new Record(0f),
            new Record("foo"),
            new Record(false, 0, 0f, "foo")
        };

        assert(schemas.length == records.length);
        for (int i = 0; i < schemas.length; ++i) {
            Schema s = schemas[i];
            Record r = records[i];
            assertEquals(r, Record.fromBytes(ByteBuffer.wrap(r.toBytes(s)), s));
        }
    }

    @Test
    public void testEquals() {
        Record a = new Record(false);
        Record b = new Record(true);
        Record c = new Record(0);

        assertEquals(a, a);
        assertNotEquals(a, b);
        assertNotEquals(a, c);
        assertNotEquals(b, a);
        assertEquals(b, b);
        assertNotEquals(b, c);
        assertNotEquals(c, a);
        assertNotEquals(c, b);
        assertEquals(c, c);
    }

    @Test
    public void testEqualsMultiple() {
        Record a = new Record(false);
        Record b = new Record(false, true);
        Record c = new Record(false, true);
        Record d = new Record(0, 1);
        Record e = new Record(false, true, "foo");
        assertEquals(b, c);
        assertEquals(c, b);
        assertNotEquals(a, b);
        assertNotEquals(b, a);
        assertNotEquals(d, b);
        assertNotEquals(b, d);
        assertNotEquals(e, b);
        assertNotEquals(b, e);
        assertNotEquals(d, e);
        assertNotEquals(e, d);
    }
}
