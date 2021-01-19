package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.MasterLogRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(SystemTests.class)
public class TestLogManager {
    private LogManager logManager;
    private BufferManager bufferManager;

    @Before
    public void setup() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        logManager = new LogManager(bufferManager);
    }

    @After
    public void cleanup() {
        logManager.close();
        bufferManager.close();
    }

    @Test
    public void testAppendFetch() {
        LogRecord expected = new MasterLogRecord(1234);

        logManager.appendToLog(expected);
        LogRecord record = logManager.fetchLogRecord(0);

        assertEquals(expected, record);
    }

    @Test
    public void testAppendScan() {
        LogRecord expected = new MasterLogRecord(1234);

        logManager.appendToLog(expected);
        LogRecord record = logManager.scanFrom(0).next();

        assertEquals(expected, record);
    }

    @Test
    public void testAppendIterator() {
        LogRecord expected = new MasterLogRecord(1234);

        logManager.appendToLog(expected);
        LogRecord record = logManager.iterator().next();

        assertEquals(expected, record);
    }

    @Test
    public void testFlushedLSN() {
        logManager.appendToLog(new MasterLogRecord(1234));
        logManager.flushToLSN(9999);

        assertEquals(9999, logManager.getFlushedLSN());
    }

    @Test
    public void testMultiPageScan() {
        for (int i = 0; i < 10000; ++i) {
            logManager.appendToLog(new MasterLogRecord(i));
        }

        Iterator<LogRecord> iter = logManager.scanFrom(90000);
        for (int i = 9 * (DiskSpaceManager.PAGE_SIZE / 9); i < 10000; ++i) {
            assertEquals(new MasterLogRecord(i), iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRewriteMasterRecord() {
        for (int i = 0; i < 1000; ++i) {
            logManager.appendToLog(new MasterLogRecord(i));
        }
        logManager.rewriteMasterRecord(new MasterLogRecord(77));
        logManager.rewriteMasterRecord(new MasterLogRecord(999));
        logManager.rewriteMasterRecord(new MasterLogRecord(-1));

        Iterator<LogRecord> iter = logManager.iterator();
        assertEquals(new MasterLogRecord(-1), iter.next());
        for (int i = 1; i < 1000; ++i) {
            assertEquals(new MasterLogRecord(i), iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test
    public void testPartialFlush() {
        for (int i = 0; i < (DiskSpaceManager.PAGE_SIZE / 9) * 7; ++i) {
            logManager.appendToLog(new MasterLogRecord(i));
        }
        Page p = bufferManager.fetchPage(new DummyLockContext((LockContext) null), 4L);
        p.unpin();
        p.flush();
        long prevIO = bufferManager.getNumIOs();
        logManager.flushToLSN(20001);
        long postIO = bufferManager.getNumIOs();
        assertEquals(3, postIO - prevIO);

        prevIO = bufferManager.getNumIOs();
        logManager.flushToLSN(50001);
        postIO = bufferManager.getNumIOs();
        assertEquals(2, postIO - prevIO);

        prevIO = bufferManager.getNumIOs();
        logManager.flushToLSN(50055);
        postIO = bufferManager.getNumIOs();
        assertEquals(0, postIO - prevIO);
    }
}
