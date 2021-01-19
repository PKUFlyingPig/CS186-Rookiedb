package edu.berkeley.cs186.database.common;

import edu.berkeley.cs186.database.categories.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category({Proj99Tests.class, SystemTests.class})
public class TestBits {
    @Test
    public void testGetBitOnByte() {
        byte b = 0b01101011;
        assertEquals(Bits.Bit.ZERO, Bits.getBit(b, 0));
        assertEquals(Bits.Bit.ONE, Bits.getBit(b, 1));
        assertEquals(Bits.Bit.ONE, Bits.getBit(b, 2));
        assertEquals(Bits.Bit.ZERO, Bits.getBit(b, 3));
        assertEquals(Bits.Bit.ONE, Bits.getBit(b, 4));
        assertEquals(Bits.Bit.ZERO, Bits.getBit(b, 5));
        assertEquals(Bits.Bit.ONE, Bits.getBit(b, 6));
        assertEquals(Bits.Bit.ONE, Bits.getBit(b, 7));
    }

    @Test
    public void testGetBitOnBytes() {
        byte[] bytes = {0b01101011, 0b01001101};

        assertEquals(Bits.Bit.ZERO, Bits.getBit(bytes, 0));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 1));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 2));
        assertEquals(Bits.Bit.ZERO, Bits.getBit(bytes, 3));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 4));
        assertEquals(Bits.Bit.ZERO, Bits.getBit(bytes, 5));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 6));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 7));

        assertEquals(Bits.Bit.ZERO, Bits.getBit(bytes, 8));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 9));
        assertEquals(Bits.Bit.ZERO, Bits.getBit(bytes, 10));
        assertEquals(Bits.Bit.ZERO, Bits.getBit(bytes, 11));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 12));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 13));
        assertEquals(Bits.Bit.ZERO, Bits.getBit(bytes, 14));
        assertEquals(Bits.Bit.ONE, Bits.getBit(bytes, 15));
    }

    @Test
    public void testSetBitOnByte() {
        assertEquals((byte) 0b10000000, Bits.setBit((byte) 0b00000000, 0, Bits.Bit.ONE));
        assertEquals((byte) 0b01000000, Bits.setBit((byte) 0b00000000, 1, Bits.Bit.ONE));
        assertEquals((byte) 0b00100000, Bits.setBit((byte) 0b00000000, 2, Bits.Bit.ONE));
        assertEquals((byte) 0b00010000, Bits.setBit((byte) 0b00000000, 3, Bits.Bit.ONE));
        assertEquals((byte) 0b00001000, Bits.setBit((byte) 0b00000000, 4, Bits.Bit.ONE));
        assertEquals((byte) 0b00000100, Bits.setBit((byte) 0b00000000, 5, Bits.Bit.ONE));
        assertEquals((byte) 0b00000010, Bits.setBit((byte) 0b00000000, 6, Bits.Bit.ONE));
        assertEquals((byte) 0b00000001, Bits.setBit((byte) 0b00000000, 7, Bits.Bit.ONE));

        assertEquals((byte) 0b01111111, Bits.setBit((byte) 0b11111111, 0, Bits.Bit.ZERO));
        assertEquals((byte) 0b10111111, Bits.setBit((byte) 0b11111111, 1, Bits.Bit.ZERO));
        assertEquals((byte) 0b11011111, Bits.setBit((byte) 0b11111111, 2, Bits.Bit.ZERO));
        assertEquals((byte) 0b11101111, Bits.setBit((byte) 0b11111111, 3, Bits.Bit.ZERO));
        assertEquals((byte) 0b11110111, Bits.setBit((byte) 0b11111111, 4, Bits.Bit.ZERO));
        assertEquals((byte) 0b11111011, Bits.setBit((byte) 0b11111111, 5, Bits.Bit.ZERO));
        assertEquals((byte) 0b11111101, Bits.setBit((byte) 0b11111111, 6, Bits.Bit.ZERO));
        assertEquals((byte) 0b11111110, Bits.setBit((byte) 0b11111111, 7, Bits.Bit.ZERO));
    }

    @Test
    public void testSetBitOnBytes() {
        byte[] bytes = {0b00000000, 0b00000000};

        // Write 1's.
        byte[][] expectedsOne = {
            {(byte) 0b10000000, (byte) 0b00000000},
            {(byte) 0b11000000, (byte) 0b00000000},
            {(byte) 0b11100000, (byte) 0b00000000},
            {(byte) 0b11110000, (byte) 0b00000000},
            {(byte) 0b11111000, (byte) 0b00000000},
            {(byte) 0b11111100, (byte) 0b00000000},
            {(byte) 0b11111110, (byte) 0b00000000},
            {(byte) 0b11111111, (byte) 0b00000000},
            {(byte) 0b11111111, (byte) 0b10000000},
            {(byte) 0b11111111, (byte) 0b11000000},
            {(byte) 0b11111111, (byte) 0b11100000},
            {(byte) 0b11111111, (byte) 0b11110000},
            {(byte) 0b11111111, (byte) 0b11111000},
            {(byte) 0b11111111, (byte) 0b11111100},
            {(byte) 0b11111111, (byte) 0b11111110},
            {(byte) 0b11111111, (byte) 0b11111111},
        };
        for (int i = 0; i < 16; ++i) {
            Bits.setBit(bytes, i, Bits.Bit.ONE);
            assertArrayEquals(expectedsOne[i], bytes);
        }

        // Write 0's.
        byte[][] expectedsZero = {
            {(byte) 0b01111111, (byte) 0b11111111},
            {(byte) 0b00111111, (byte) 0b11111111},
            {(byte) 0b00011111, (byte) 0b11111111},
            {(byte) 0b00001111, (byte) 0b11111111},
            {(byte) 0b00000111, (byte) 0b11111111},
            {(byte) 0b00000011, (byte) 0b11111111},
            {(byte) 0b00000001, (byte) 0b11111111},
            {(byte) 0b00000000, (byte) 0b11111111},
            {(byte) 0b00000000, (byte) 0b01111111},
            {(byte) 0b00000000, (byte) 0b00111111},
            {(byte) 0b00000000, (byte) 0b00011111},
            {(byte) 0b00000000, (byte) 0b00001111},
            {(byte) 0b00000000, (byte) 0b00000111},
            {(byte) 0b00000000, (byte) 0b00000011},
            {(byte) 0b00000000, (byte) 0b00000001},
            {(byte) 0b00000000, (byte) 0b00000000},
        };
        for (int i = 0; i < 16; ++i) {
            Bits.setBit(bytes, i, Bits.Bit.ZERO);
            assertArrayEquals(expectedsZero[i], bytes);
        }
    }
}
