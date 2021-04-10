package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.databox.Type;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestSchema {
    @Test
    public void testSizeInBytes() {
        Schema[] schemas = {
            // Single column.
            new Schema().add("x", Type.boolType()),
            new Schema().add("x", Type.intType()),
            new Schema().add("x", Type.floatType()),
            new Schema().add("x", Type.stringType(1)),
            new Schema().add("x", Type.stringType(10)),

            // Multiple columns.
            new Schema()
                .add("x", Type.boolType())
                .add("y", Type.intType())
                .add("z", Type.floatType()),
            new Schema()
                .add("x", Type.boolType())
                .add("y", Type.stringType(42))
        };

        int[] expectedSizes = {1, 4, 4, 1, 10, 9, 43};

        assert(schemas.length == expectedSizes.length);
        for (int i = 0; i < schemas.length; ++i) {
            assertEquals(expectedSizes[i], schemas[i].getSizeInBytes());
        }
    }

    @Test
    public void testVerifyValidRecords() {
        try {
            Schema[] schemas = {
                new Schema(),
                new Schema().add("x", Type.boolType()),
                new Schema().add("x", Type.intType()),
                new Schema().add("x", Type.floatType()),
                new Schema().add("x", Type.stringType(1)),
                new Schema().add("x", Type.stringType(2)),
            };
            Record[] records = {
                    new Record(),
                    new Record(false),
                    new Record(0),
                    new Record(0f),
                    new Record("a"),
                    new Record("ab")
            };

            assert(schemas.length == records.length);
            for (int i = 0; i < schemas.length; ++i) {
                Schema s = schemas[i];
                Record r = records[i];
                assertEquals(r, s.verify(r));
            }
        } catch (DatabaseException e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = DatabaseException.class)
    public void testVerifyWrongSize() {
        Schema schema = new Schema().add("x", Type.boolType());
        Record empty = new Record();
        schema.verify(empty);
    }

    @Test(expected = DatabaseException.class)
    public void testVerifyWrongType() {
        Schema floatSchema = new Schema().add("x", Type.boolType());
        Record intRecord = new Record(42);
        floatSchema.verify(intRecord);
    }

    @Test
    public void testToAndFromBytes() {
        Schema[] schemas = {
            // Single column.
            new Schema().add("x", Type.intType()),
            new Schema().add("x", Type.floatType()),
            new Schema().add("x", Type.boolType()),
            new Schema().add("x", Type.stringType(1)),
            new Schema().add("x", Type.stringType(10)),

            // Multiple columns.
            new Schema()
                .add("x", Type.boolType())
                .add("y", Type.intType())
                .add("z", Type.floatType()),
            new Schema()
                .add("x", Type.boolType())
                .add("y", Type.stringType(42))
        };

        for (Schema schema : schemas) {
            assertEquals(schema, Schema.fromBytes(ByteBuffer.wrap(schema.toBytes())));
        }
    }

    @Test
    public void testEquals() {
        Schema b = new Schema().add("y", Type.intType());
        Schema c = new Schema().add("x", Type.boolType());
        Schema a = new Schema().add("x", Type.intType());

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
}
