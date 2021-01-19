package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.recovery.records.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(SystemTests.class)
public class TestLogRecord {
    private void checkSerialize(LogRecord record) {
        assertEquals(record, LogRecord.fromBytes(ByteBuffer.wrap(record.toBytes())).orElse(null));
    }

    @Test
    public void testMasterSerialize() {
        checkSerialize(new MasterLogRecord(-98765L));
    }

    @Test
    public void testAbortTransactionSerialize() {
        checkSerialize(new AbortTransactionLogRecord(-98765L, -43210L));
    }

    @Test
    public void testCommitTransactionSerialize() {
        checkSerialize(new CommitTransactionLogRecord(-98765L, -43210L));
    }

    @Test
    public void testEndTransactionSerialize() {
        checkSerialize(new EndTransactionLogRecord(-98765L, -43210L));
    }

    @Test
    public void testAllocPageSerialize() {
        checkSerialize(new AllocPageLogRecord(-98765L, -43210L, -77654L));
    }

    @Test
    public void testFreePageSerialize() {
        checkSerialize(new FreePageLogRecord(-98765L, -43210L, -77654L));
    }

    @Test
    public void testAllocPartSerialize() {
        checkSerialize(new AllocPartLogRecord(-98765L, -43210, -77654L));
    }

    @Test
    public void testFreePartSerialize() {
        checkSerialize(new FreePartLogRecord(-98765L, -43210, -77654L));
    }

    @Test
    public void testUndoAllocPageSerialize() {
        checkSerialize(new UndoAllocPageLogRecord(-98765L, -43210L, -77654L, -91235L));
    }

    @Test
    public void testUndoFreePageSerialize() {
        checkSerialize(new UndoFreePageLogRecord(-98765L, -43210L, -77654L, -91235L));
    }

    @Test
    public void testUndoAllocPartSerialize() {
        checkSerialize(new UndoAllocPartLogRecord(-98765L, -43210, -77654L, -91235L));
    }

    @Test
    public void testUndoFreePartSerialize() {
        checkSerialize(new UndoFreePartLogRecord(-98765L, -43210, -77654L, -91235L));
    }

    @Test
    public void testUpdatePageSerialize() {
        checkSerialize(new UpdatePageLogRecord(-98765L, -43210L, -12345L, (short) 1234, "asdfg".getBytes(),
                                               "zxcvb".getBytes()));
        checkSerialize(new UpdatePageLogRecord(-98765L, -43210L, -12345L, (short) 1234, null,
                                               "zxcvb".getBytes()));
        checkSerialize(new UpdatePageLogRecord(-98765L, -43210L, -12345L, (short) 1234, "asdfg".getBytes(),
                                               null));
    }

    @Test
    public void testUndoUpdatePageSerialize() {
        byte[] pageString = new String(new char[BufferManager.EFFECTIVE_PAGE_SIZE]).replace('\0',
                'a').getBytes();
        checkSerialize(new UndoUpdatePageLogRecord(-98765L, -43210L, -12345L, -57812L, (short) 0,
                       "zxcvb".getBytes()));
        checkSerialize(new UndoUpdatePageLogRecord(-98765L, -43210L, -12345L, -57812L, (short) 1234,
                       "zxcvb".getBytes()));
        checkSerialize(new UndoUpdatePageLogRecord(-98765L, -43210L, -12345L, -57812L, (short) 0,
                       pageString));
    }

    @Test
    public void testBeginCheckpointSerialize() {
        checkSerialize(new BeginCheckpointLogRecord(92587213L));
    }

    @Test
    public void testEndCheckpointSerialize() {
        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> xacts = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();

        checkSerialize(new EndCheckpointLogRecord(dpt, xacts, touchedPages));

        for (long i = 0; i < 100; ++i) {
            dpt.put(i, i);
        }

        checkSerialize(new EndCheckpointLogRecord(dpt, xacts, touchedPages));

        for (long i = 0; i < 53; ++i) {
            xacts.put(i, new Pair<>(Transaction.Status.RUNNING, i));
            xacts.put(i, new Pair<>(Transaction.Status.COMMITTING, i));
            xacts.put(i, new Pair<>(Transaction.Status.ABORTING, i));
        }

        checkSerialize(new EndCheckpointLogRecord(dpt, xacts, touchedPages));

        for (long i = 43; i < 63; ++i) {
            touchedPages.put(i, new ArrayList<>());
            for (long j = 43; j < i; ++j) {
                touchedPages.get(i).add(j);
            }
        }

        checkSerialize(new EndCheckpointLogRecord(dpt, xacts, touchedPages));

        dpt.clear();
        checkSerialize(new EndCheckpointLogRecord(dpt, xacts, touchedPages));

        xacts.clear();
        checkSerialize(new EndCheckpointLogRecord(dpt, xacts, touchedPages));
    }
}
