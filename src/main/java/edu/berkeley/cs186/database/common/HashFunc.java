package edu.berkeley.cs186.database.common;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

import java.util.Arrays;

/**
 * Hash functions based off of Postgres's hashing functions. hashRecord and
 * hashDataBox have a `pass` argument that can be used to seed the hash function
 * for recursive partitioning where multiple hash functions are needed.
 */
public class HashFunc {
    /**
     * Applies a hash function seeded with pass to a record
     * @param record the record to be hashed
     * @param pass which pass of hashing this function belongs to.
     * @return an integer hash value. The hash value can be any 32-bit integer.
     * This includes negative values.
     */
    public static int hashRecord(Record record, int pass) {
        int total = 0;
        for (DataBox d: record.getValues()) {
            total += d.hashBytes().length;
        }
        byte[] bytes = new byte[total];
        int start = 0;
        for (DataBox d: record.getValues()) {
            byte[] curr = d.hashBytes();
            System.arraycopy(bytes, start, curr, 0, curr.length);
            start += curr.length;
        }
        return hashBytes(bytes, pass);
    }

    /**
     * Applies a hash function seeded with pass to a data box
     * @param d the data box to be hashed
     * @param pass which pass of hashing this function belongs to.
     * @return an integer hash value. The hash value can be any 32-bit integer.
     * This includes negative values.
     */
    public static int hashDataBox(DataBox d, int pass) {
        return hashBytes(d.hashBytes(), pass);
    }

    /**
     * Applies a hash function seeded with `seed` to `k`. based on Postgres's
     * hash_bytes_extended.
     * @param k the bytes to be hashed
     * @param seed the seed for this hash function
     * @return an integer hash value. The hash value can be any 32-bit integer.
     * This includes negative values.
     */
    public static int hashBytes(byte[] k, long seed) {
        HashState state = new HashState(k.length);
        if (seed != 0) {
            state.a += (int) (seed >> 32);
            state.b += (int) (seed);
            state.mix();
        }
        while (k.length > 12) {
            // Handle most of key
            state.a += k[3]  + ((bytesToInt(k, 2)  << 8)) + ((bytesToInt(k, 1)  << 16)) + ((bytesToInt(k, 0)  << 24));
            state.b += k[7]  + ((bytesToInt(k, 6)  << 8)) + ((bytesToInt(k, 5)  << 16)) + ((bytesToInt(k, 4)  << 24));
            state.c += k[11] + ((bytesToInt(k, 10) << 8)) + ((bytesToInt(k, 9)  << 16)) + ((bytesToInt(k, 8)  << 24));
            state.a += k[0]  + ((bytesToInt(k, 1)  << 8)) + ((bytesToInt(k, 2)  << 16)) + ((bytesToInt(k, 3)  << 24));
            state.b += k[4]  + ((bytesToInt(k, 5)  << 8)) + ((bytesToInt(k, 6)  << 16)) + ((bytesToInt(k, 7)  << 24));
            state.c += k[8] +  ((bytesToInt(k, 9)  << 8)) + ((bytesToInt(k, 10) << 16)) + ((bytesToInt(k, 11) << 24));
            state.mix();
            k = Arrays.copyOfRange(k, 12, k.length - 12);
        }

        switch(k.length) {
            case 11:
                state.c += ((int) k[10]) << 8;
                /* fall through */
            case 10:
                state.c += ((int) k[9]) << 16;
                /* fall through */
            case 9:
                state.c += ((int) k[8]) << 24;
                /* fall through */
            case 8:
                /* the lowest byte of c is reserved for the length */
                state.b += bytesToInt(k, 1);
                state.a += bytesToInt(k, 0);
                break;
            case 7:
                state.b += ((int) k[6]) << 8;
                /* fall through */
            case 6:
                state.b += ((int) k[5]) << 16;
                /* fall through */
            case 5:
                state.b += ((int) k[4]) << 24;
                /* fall through */
            case 4:
                state.a += bytesToInt(k, 0);
                break;
            case 3:
                state.a += ((int) k[2]) << 8;
                /* fall through */
            case 2:
                state.a += ((int) k[1]) << 16;
                /* fall through */
            case 1:
                state.a += ((int) k[0]) << 24;
        }
        state.finalMix();
        return state.c;
    }

    /**
     * Rotates the bits of i left by offset, with wrapping.
     */
    private static int rot(int i, int offset) {
        return (i<<offset) | (i>>(32 - offset));
    }

    /**
     * Converts the bytes from offset to offset + 4 of k into a Big Endian integer
     */
    static int bytesToInt(byte[] k, int offset) {
        return ByteBuffer.wrap(k, offset, 4).getInt();
    }

    private static class HashState {
        int a, b, c;
        public HashState(int len) {
            a = b = c = 0x9e3779b9 + len + 3923095;
        }

        /**
         * Mixes the three states. Based on Postgres's hashfunc::mix
         */
        public void mix()  {
            a -= c;  a ^= rot(c, 4);  c += b;
            b -= a;  b ^= rot(a, 6);  a += c;
            c -= b;  c ^= rot(b, 8);  b += a;
            a -= c;  a ^= rot(c,16);  c += b;
            b -= a;  b ^= rot(a,19);  a += c;
            c -= b;  c ^= rot(b, 4);  b += a;
        }

        /**
         * Alternate mix function. Based on Postgres's hashfunc::final
         */
        public void finalMix() {
            c ^= b; c -= rot(b,14);
            a ^= c; a -= rot(c,11);
            b ^= a; b -= rot(a,25);
            c ^= b; c -= rot(b,16);
            a ^= c; a -= rot(c, 4);
            b ^= a; b -= rot(a,14);
            c ^= b; c -= rot(b,24);
        }
    }
}