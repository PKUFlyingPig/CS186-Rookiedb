package edu.berkeley.cs186.database.common;

/**
 * Utilities for getting, setting, and counting bits within a byte or array of
 * bytes.
 */
public class Bits {
    public enum Bit { ZERO, ONE }

    /**
     * Get the ith bit of a byte where the 0th bit is the most significant bit
     * and the 7th bit is the least significant bit. Some examples:
     *
     *   - getBit(0b10000000, 7) == ZERO
     *   - getBit(0b10000000, 0) == ONE
     *   - getBit(0b01000000, 1) == ONE
     *   - getBit(0b00100000, 1) == ZERO
     */
    static Bit getBit(byte b, int i) {
        if (i < 0 || i >= 8) {
            throw new IllegalArgumentException(String.format("index %d out of bounds", i));
        }
        return ((b >> (7 - i)) & 1) == 0 ? Bit.ZERO : Bit.ONE;
    }

    /**
     * Get the ith bit of a byte array where the 0th bit is the most significat
     * bit of the first byte. Some examples:
     *
     *   - getBit(new byte[]{0b10000000, 0b00000000}, 0) == ONE
     *   - getBit(new byte[]{0b01000000, 0b00000000}, 1) == ONE
     *   - getBit(new byte[]{0b00000000, 0b00000001}, 15) == ONE
     */
    public static Bit getBit(byte[] bytes, int i) {
        if (bytes.length == 0 || i < 0 || i >= bytes.length * 8) {
            String err = String.format("bytes.length = %d; i = %d.", bytes.length, i);
            throw new IllegalArgumentException(err);
        }
        return getBit(bytes[i / 8], i % 8);
    }

    /**
     * Set the ith bit of a byte where the 0th bit is the most significant bit
     * and the 7th bit is the least significant bit. Some examples:
     *
     *   - setBit(0b00000000, 0, ONE) == 0b10000000
     *   - setBit(0b00000000, 1, ONE) == 0b01000000
     *   - setBit(0b00000000, 2, ONE) == 0b00100000
     */
    static byte setBit(byte b, int i, Bit bit) {
        if (i < 0 || i >= 8) {
            throw new IllegalArgumentException(String.format("index %d out of bounds", i));
        }
        byte mask = (byte) (1 << (7 - i));
        switch (bit) {
        case ZERO: { return (byte) (b & ~mask); }
        case ONE: { return (byte) (b | mask); }
        default: { throw new IllegalArgumentException("Unreachable code."); }
        }
    }

    /**
     * Set the ith bit of a byte array where the 0th bit is the most significant
     * bit of the first byte (arr[0]). An example:
     *
     *   byte[] buf = new bytes[2]; // [0b00000000, 0b00000000]
     *   setBit(buf, 0, ONE); // [0b10000000, 0b00000000]
     *   setBit(buf, 1, ONE); // [0b11000000, 0b00000000]
     *   setBit(buf, 2, ONE); // [0b11100000, 0b00000000]
     *   setBit(buf, 15, ONE); // [0b11100000, 0b00000001]
     */
    public static void setBit(byte[] bytes, int i, Bit bit) {
        bytes[i / 8] = setBit(bytes[i / 8], i % 8, bit);
    }

    /**
     * Counts the number of set bits. For example:
     *
     *   - countBits(0b00001010) == 2
     *   - countBits(0b11111101) == 7
     */
    public static int countBits(byte b) {
        return Integer.bitCount(b);
    }

    /**
     * Counts the number of set bits.
     */
    public static int countBits(byte[] bytes) {
        int count = 0;
        for (byte b : bytes) {
            count += countBits(b);
        }
        return count;
    }
}
