package edu.berkeley.cs186.database.databox;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.ByteBuffer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestStringDataBox {
    @Test(expected = IllegalArgumentException.class)
    public void testEmptyString() {
        new StringDataBox("", 0);
    }

    @Test
    public void testType() {
        assertEquals(Type.stringType(3), new StringDataBox("foo", 3).type());
    }

    @Test(expected = RuntimeException.class)
    public void testGetBool() {
        new StringDataBox("foo", 3).getBool();
    }

    @Test(expected = RuntimeException.class)
    public void testGetInt() {
        new StringDataBox("foo", 3).getInt();
    }

    @Test(expected = RuntimeException.class)
    public void testGetLong() {
        new StringDataBox("foo", 3).getLong();
    }

    @Test(expected = RuntimeException.class)
    public void testGetFloat() {
        new StringDataBox("foo", 3).getFloat();
    }

    @Test
    public void testGetString() {
        assertEquals("f", new StringDataBox("foo", 1).getString());
        assertEquals("fo", new StringDataBox("foo", 2).getString());
        assertEquals("foo", new StringDataBox("foo", 3).getString());
        assertEquals("foo", new StringDataBox("foo", 4).getString());
        assertEquals("foo", new StringDataBox("foo", 5).getString());
    }

    @Test
    public void testToAndFromBytes() {
        for (String s : new String[] {"foo", "bar", "baz"}) {
            StringDataBox d = new StringDataBox(s, 3);
            byte[] bytes = d.toBytes();
            assertEquals(d, DataBox.fromBytes(ByteBuffer.wrap(bytes), Type.stringType(3)));
        }
    }

    @Test
    public void testEquals() {
        StringDataBox foo = new StringDataBox("foo", 3);
        StringDataBox zoo = new StringDataBox("zoo", 3);
        assertEquals(foo, foo);
        assertEquals(zoo, zoo);
        assertNotEquals(foo, zoo);
        assertNotEquals(zoo, foo);
    }

    @Test
    public void testCompareTo() {
        StringDataBox foo = new StringDataBox("foo", 3);
        StringDataBox zoo = new StringDataBox("zoo", 3);
        assertTrue(foo.compareTo(foo) == 0);
        assertTrue(foo.compareTo(zoo) < 0);
        assertTrue(zoo.compareTo(zoo) == 0);
        assertTrue(zoo.compareTo(foo) > 0);
    }
}
