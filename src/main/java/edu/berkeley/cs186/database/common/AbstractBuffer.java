package edu.berkeley.cs186.database.common;

import java.nio.ByteBuffer;

/**
 * Partial implementation of a Buffer, which funnels all operations
 * into the generic get and put operation.
 */
public abstract class AbstractBuffer implements Buffer {
    private int pos;
    private byte[] bytes;
    private ByteBuffer buf;

    protected AbstractBuffer(int pos) {
        this.pos = pos;
        this.bytes = new byte[8];
        this.buf = ByteBuffer.wrap(this.bytes);
    }

    @Override
    public abstract Buffer get(byte[] dst, int offset, int length);

    @Override
    public final byte get(int index) {
        get(bytes, index, 1);
        return bytes[0];
    }

    @Override
    public final byte get() {
        ++this.pos;
        return get(this.pos - 1);
    }

    @Override
    public final Buffer get(byte[] dst) {
        pos += dst.length;
        return get(dst, pos - dst.length, dst.length);
    }

    @Override
    public final char getChar() {
        ++this.pos;
        return getChar(this.pos - 1);
    }

    @Override
    public final char getChar(int index) {
        get(bytes, index, 1);
        return buf.getChar(0);
    }

    @Override
    public final double getDouble() {
        this.pos += 8;
        return getDouble(this.pos - 8);
    }

    @Override
    public final double getDouble(int index) {
        get(bytes, index, 8);
        return buf.getDouble(0);
    }

    @Override
    public final float getFloat() {
        this.pos += 4;
        return getFloat(this.pos - 4);
    }

    @Override
    public final float getFloat(int index) {
        get(bytes, index, 4);
        return buf.getFloat(0);
    }

    @Override
    public final int getInt() {
        this.pos += 4;
        return getInt(this.pos - 4);
    }

    @Override
    public final int getInt(int index) {
        get(bytes, index, 4);
        return buf.getInt(0);
    }

    @Override
    public final long getLong() {
        this.pos += 8;
        return getLong(this.pos - 8);
    }

    @Override
    public final long getLong(int index) {
        get(bytes, index, 8);
        return buf.getLong(0);
    }

    @Override
    public final short getShort() {
        this.pos += 2;
        return getShort(this.pos - 2);
    }

    @Override
    public final short getShort(int index) {
        get(bytes, index, 2);
        return buf.getShort(0);
    }

    @Override
    public abstract Buffer put(byte[] src, int offset, int length);

    @Override
    public final Buffer put(byte[] src) {
        pos += src.length;
        return put(src, pos - src.length, src.length);
    }

    @Override
    public final Buffer put(byte b) {
        ++pos;
        return put(pos - 1, b);
    }

    @Override
    public final Buffer put(int index, byte b) {
        bytes[0] = b;
        return put(bytes, index, 1);
    }

    @Override
    public final Buffer putChar(char value) {
        ++pos;
        return putChar(pos - 1, value);
    }

    @Override
    public final Buffer putChar(int index, char value) {
        buf.putChar(0, value);
        return put(bytes, index, 1);
    }

    @Override
    public final Buffer putDouble(double value) {
        pos += 8;
        return putDouble(pos - 8, value);
    }

    @Override
    public final Buffer putDouble(int index, double value) {
        buf.putDouble(0, value);
        return put(bytes, index, 8);
    }

    @Override
    public final Buffer putFloat(float value) {
        pos += 4;
        return putFloat(pos - 4, value);
    }

    @Override
    public final Buffer putFloat(int index, float value) {
        buf.putFloat(0, value);
        return put(bytes, index, 4);
    }

    @Override
    public final Buffer putInt(int value) {
        pos += 4;
        return putInt(pos - 4, value);
    }

    @Override
    public final Buffer putInt(int index, int value) {
        buf.putInt(0, value);
        return put(bytes, index, 4);
    }

    @Override
    public final Buffer putLong(long value) {
        pos += 8;
        return putLong(pos - 8, value);
    }

    @Override
    public final Buffer putLong(int index, long value) {
        buf.putLong(0, value);
        return put(bytes, index, 8);
    }

    @Override
    public final Buffer putShort(short value) {
        pos += 2;
        return putShort(pos - 2, value);
    }

    @Override
    public final Buffer putShort(int index, short value) {
        buf.putShort(0, value);
        return put(bytes, index, 2);
    }

    @Override
    public abstract Buffer slice();

    @Override
    public abstract Buffer duplicate();

    @Override
    public final int position() {
        return this.pos;
    }

    @Override
    public final Buffer position(int pos) {
        this.pos = pos;
        return this;
    }
}
