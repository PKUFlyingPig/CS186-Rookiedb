package edu.berkeley.cs186.database.databox;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.ByteBuffer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestFloatDataBox {
    @Test
    public void testType() {
        assertEquals(Type.floatType(), new FloatDataBox(0f).type());
    }

    @Test(expected = RuntimeException.class)
    public void testGetBool() {
        new FloatDataBox(0f).getBool();
    }

    @Test(expected = RuntimeException.class)
    public void testGetInt() {
        new FloatDataBox(0f).getInt();
    }

    @Test(expected = RuntimeException.class)
    public void testGetLong() {
        new FloatDataBox(0f).getLong();
    }

    @Test
    public void testGetFloat() {
        assertEquals(0f, new FloatDataBox(0f).getFloat(), 0.0001);
    }

    @Test(expected = RuntimeException.class)
    public void testGetString() {
        new FloatDataBox(0f).getString();
    }

    @Test
    public void testToAndFromBytes() {
        for (int i = -10; i < 10; ++i) {
            FloatDataBox d = new FloatDataBox((float) i);
            byte[] bytes = d.toBytes();
            assertEquals(d, DataBox.fromBytes(ByteBuffer.wrap(bytes), Type.floatType()));
        }
    }

    @Test
    public void testEquals() {
        FloatDataBox zero = new FloatDataBox(0f);
        FloatDataBox one = new FloatDataBox(1f);
        assertEquals(zero, zero);
        assertEquals(one, one);
        assertNotEquals(zero, one);
        assertNotEquals(one, zero);
    }

    @Test
    public void testCompareTo() {
        FloatDataBox zero = new FloatDataBox(0f);
        FloatDataBox one = new FloatDataBox(1f);
        assertTrue(zero.compareTo(zero) == 0);
        assertTrue(zero.compareTo(one) < 0);
        assertTrue(one.compareTo(one) == 0);
        assertTrue(one.compareTo(zero) > 0);
    }
}
