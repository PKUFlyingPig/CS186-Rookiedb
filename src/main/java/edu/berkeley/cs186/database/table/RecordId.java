package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.common.Buffer;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A Record in a particular table is uniquely identified by its page number
 * (the number of the page on which it resides) and its entry number (the
 * record's index in the page). A RecordId is a pair of the page number and
 * entry number.
 */
public class RecordId implements Comparable<RecordId> {
    private long pageNum;
    private short entryNum;

    public RecordId(long pageNum, short entryNum) {
        this.pageNum = pageNum;
        this.entryNum = entryNum;
    }

    public long getPageNum() {
        return this.pageNum;
    }

    public short getEntryNum() {
        return this.entryNum;
    }

    public static int getSizeInBytes() {
        // See toBytes.
        return Long.BYTES + Short.BYTES;
    }

    public byte[] toBytes() {
        // A RecordId is serialized as its 8-byte page number followed by its
        // 2-byte short.
        return ByteBuffer.allocate(getSizeInBytes())
               .putLong(pageNum)
               .putShort(entryNum)
               .array();
    }

    public static RecordId fromBytes(Buffer buf) {
        return new RecordId(buf.getLong(), buf.getShort());
    }

    @Override
    public String toString() {
        return String.format("RecordId(%d, %d)", pageNum, entryNum);
    }

    public String toSexp() {
        return String.format("(%d %d)", pageNum, entryNum);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof RecordId)) return false;
        RecordId r = (RecordId) o;
        return pageNum == r.pageNum && entryNum == r.entryNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageNum, entryNum);
    }

    @Override
    public int compareTo(RecordId r) {
        int x = Long.compare(pageNum, r.pageNum);
        return x == 0 ? Integer.compare(entryNum, r.entryNum) : x;
    }
}
