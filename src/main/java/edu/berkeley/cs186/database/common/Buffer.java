package edu.berkeley.cs186.database.common;

/**
 * Buffers are used to store and sequences of bytes, for example when we want
 * to serialize information into a byte sequence that can be stored on disk
 * and deserialize the sequence back into a Java object. Put methods will return
 * the buffer itself allowing you to chain together calls to put. For example,
 * calling:
 *
 * ByteBuffer b = ByteBuffer.allocate(6);
 * // Buffer contents: empty
 * b.putChar('c').putChar('s').putInt(186);
 * // Buffer contents: |0x63, 0x73, 0x00, 0x00, 0x00, 0xBA|
 *
 * Calling get will deserialize bytes from the beginning of the buffer (or the
 * specified index) and move the beginning of the buffer to the next unread
 * byte. Reusing the buffer from above:
 *
 * char char1 = b.getChar(); // char1 = 'c'
 * // Buffer contents: |0x73, 0x00, 0x00, 0x00, 0xBA|
 * char char2 = b.getChar(): // char2 = 's'
 * // Buffer contents: |0x00, 0x00, 0x00, 0xBA|
 * int num = b.getInt(): // num = 186
 * // Buffer contents: empty
 *
 * The buffer has no way of knowing what the original data types were, so
 * its important that you deserialize the contents in the same way as it was
 * serialized. For example, calling b.getInt() immediately would have attempted
 * to read the first 4 bytes (0x63, 0x73, 0x00, 0x00) in as an integer, despite
 * the first two bytes being part of characters, and not an integer.
 *
 * In general you'll want to call your get operations in the same order as the
 * put operations took place.
 */
public interface Buffer {
    Buffer get(byte[] dst, int offset, int length);
    byte get(int index);
    byte get();
    Buffer get(byte[] dst);
    char getChar();
    char getChar(int index);
    double getDouble();
    double getDouble(int index);
    float getFloat();
    float getFloat(int index);
    int getInt();
    int getInt(int index);
    long getLong();
    long getLong(int index);
    short getShort();
    short getShort(int index);
    Buffer put(byte[] src, int offset, int length);
    Buffer put(byte[] src);
    Buffer put(byte b);
    Buffer put(int index, byte b);
    Buffer putChar(char value);
    Buffer putChar(int index, char value);
    Buffer putDouble(double value);
    Buffer putDouble(int index, double value);
    Buffer putFloat(float value);
    Buffer putFloat(int index, float value);
    Buffer putInt(int value);
    Buffer putInt(int index, int value);
    Buffer putLong(long value);
    Buffer putLong(int index, long value);
    Buffer putShort(short value);
    Buffer putShort(int index, short value);
    Buffer slice();
    Buffer duplicate();
    int position();
    Buffer position(int pos);
}
