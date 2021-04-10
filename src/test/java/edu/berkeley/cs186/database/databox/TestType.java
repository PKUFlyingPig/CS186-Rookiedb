package edu.berkeley.cs186.database.databox;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category({Proj99Tests.class, SystemTests.class})
public class TestType {
    @Test
    public void testBoolType() {
        // Check type id and size.
        Type boolType = Type.boolType();
        assertEquals(boolType.getTypeId(), TypeId.BOOL);
        assertEquals(boolType.getSizeInBytes(), 1);

        // Check toBytes and fromBytes.
        Buffer buf = ByteBuffer.wrap(boolType.toBytes());
        assertEquals(boolType, Type.fromBytes(buf));

        // Check equality.
        assertEquals(boolType, Type.boolType());
        assertNotEquals(boolType, Type.intType());
        assertNotEquals(boolType, Type.floatType());
        assertNotEquals(boolType, Type.stringType(1));
        assertNotEquals(boolType, Type.stringType(2));
    }

    @Test
    public void testIntType() {
        // Check type id and size.
        Type intType = Type.intType();
        assertEquals(intType.getTypeId(), TypeId.INT);
        assertEquals(intType.getSizeInBytes(), 4);

        // Check toBytes and fromBytes.
        Buffer buf = ByteBuffer.wrap(intType.toBytes());
        assertEquals(intType, Type.fromBytes(buf));

        // Check equality.
        assertNotEquals(intType, Type.boolType());
        assertEquals(intType, Type.intType());
        assertNotEquals(intType, Type.floatType());
        assertNotEquals(intType, Type.stringType(1));
        assertNotEquals(intType, Type.stringType(2));
    }

    @Test
    public void testFloatType() {
        // Check type id and size.
        Type floatType = Type.floatType();
        assertEquals(floatType.getTypeId(), TypeId.FLOAT);
        assertEquals(floatType.getSizeInBytes(), 4);

        // Check toBytes and fromBytes.
        Buffer buf = ByteBuffer.wrap(floatType.toBytes());
        assertEquals(floatType, Type.fromBytes(buf));

        // Check equality.
        assertNotEquals(floatType, Type.boolType());
        assertNotEquals(floatType, Type.intType());
        assertEquals(floatType, Type.floatType());
        assertNotEquals(floatType, Type.stringType(1));
        assertNotEquals(floatType, Type.stringType(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroByteStringype() {
        Type.stringType(0);
    }

    @Test
    public void testOneByteStringype() {
        // Check type id and size.
        Type stringType = Type.stringType(1);
        assertEquals(stringType.getTypeId(), TypeId.STRING);
        assertEquals(stringType.getSizeInBytes(), 1);

        // Check toBytes and fromBytes.
        Buffer buf = ByteBuffer.wrap(stringType.toBytes());
        assertEquals(stringType, Type.fromBytes(buf));

        // Check equality.
        assertNotEquals(stringType, Type.boolType());
        assertNotEquals(stringType, Type.intType());
        assertNotEquals(stringType, Type.floatType());
        assertEquals(stringType, Type.stringType(1));
        assertNotEquals(stringType, Type.stringType(2));
    }

    @Test
    public void testTwoByteStringype() {
        // Check type id and size.
        Type stringType = Type.stringType(2);
        assertEquals(stringType.getTypeId(), TypeId.STRING);
        assertEquals(stringType.getSizeInBytes(), 2);

        // Check toBytes and fromBytes.
        Buffer buf = ByteBuffer.wrap(stringType.toBytes());
        assertEquals(stringType, Type.fromBytes(buf));

        // Check equality.
        assertNotEquals(stringType, Type.boolType());
        assertNotEquals(stringType, Type.intType());
        assertNotEquals(stringType, Type.floatType());
        assertNotEquals(stringType, Type.stringType(1));
        assertEquals(stringType, Type.stringType(2));
    }
}
