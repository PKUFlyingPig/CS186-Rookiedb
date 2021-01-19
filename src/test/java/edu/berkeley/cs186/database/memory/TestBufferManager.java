package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestBufferManager {
    private DiskSpaceManager diskSpaceManager;
    private BufferManager bufferManager;

    @Before
    public void beforeEach() {
        diskSpaceManager = new MemoryDiskSpaceManager();
        bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 5,
                                              new ClockEvictionPolicy());
    }

    @After
    public void afterEach() {
        bufferManager.close();
        diskSpaceManager.close();
    }

    @Test
    public void testFetchNewPage() {
        int partNum = diskSpaceManager.allocPart(1);

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame2 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame3 = bufferManager.fetchNewPageFrame(partNum);
        frame1.unpin();
        frame2.unpin();
        frame3.unpin();

        assertTrue(frame1.isValid());
        assertTrue(frame2.isValid());
        assertTrue(frame3.isValid());

        BufferFrame frame4 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame5 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame6 = bufferManager.fetchNewPageFrame(partNum);
        frame4.unpin();
        frame5.unpin();
        frame6.unpin();

        assertFalse(frame1.isValid());
        assertTrue(frame2.isValid());
        assertTrue(frame3.isValid());
        assertTrue(frame4.isValid());
        assertTrue(frame5.isValid());
        assertTrue(frame6.isValid());
    }

    @Test
    public void testFetchPage() {
        int partNum = diskSpaceManager.allocPart(1);

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame2 = bufferManager.fetchPageFrame(frame1.getPageNum());

        frame1.unpin();
        frame2.unpin();

        assertSame(frame1, frame2);
    }

    @Test
    public void testReadWrite() {
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[4];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.readBytes((short) 67, (short) 4, actual);
        frame1.unpin();

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testFlush() {
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[DiskSpaceManager.PAGE_SIZE];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.unpin();

        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(new byte[4], Arrays.copyOfRange(actual, 67 + BufferManager.RESERVED_SPACE,
                          71 + BufferManager.RESERVED_SPACE));

        frame1.flush();
        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(expected, Arrays.copyOfRange(actual, 67 + BufferManager.RESERVED_SPACE,
                          71 + BufferManager.RESERVED_SPACE));

        frame1.pin();
        frame1.writeBytes((short) 33, (short) 4, expected);
        frame1.unpin();

        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(new byte[4], Arrays.copyOfRange(actual, 33 + BufferManager.RESERVED_SPACE,
                          37 + BufferManager.RESERVED_SPACE));

        // force a eviction
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();

        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertFalse(frame1.isValid());
        assertArrayEquals(expected, Arrays.copyOfRange(actual, 33 + BufferManager.RESERVED_SPACE,
                          37 + BufferManager.RESERVED_SPACE));
    }

    @Test
    public void testFlushLogPage() {
        int partNum = diskSpaceManager.allocPart(0);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[DiskSpaceManager.PAGE_SIZE];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.unpin();

        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(new byte[4], Arrays.copyOfRange(actual, 67, 71));

        frame1.flush();
        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(expected, Arrays.copyOfRange(actual, 67, 71));

        frame1.pin();
        frame1.writeBytes((short) 33, (short) 4, expected);
        frame1.unpin();

        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertArrayEquals(new byte[4], Arrays.copyOfRange(actual, 33, 37));

        // force a eviction
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();

        diskSpaceManager.readPage(frame1.getPageNum(), actual);
        assertFalse(frame1.isValid());
        assertArrayEquals(expected, Arrays.copyOfRange(actual, 33, 37));
    }

    @Test
    public void testReload() {
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[4];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.unpin();

        // force a eviction
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();

        assertFalse(frame1.isValid());

        // reload page
        frame1 = bufferManager.fetchPageFrame(frame1.getPageNum());
        frame1.readBytes((short) 67, (short) 4, actual);
        frame1.unpin();

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testRequestValidFrame() {
        int partNum = diskSpaceManager.allocPart(1);

        byte[] expected = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        byte[] actual = new byte[4];

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        frame1.writeBytes((short) 67, (short) 4, expected);
        frame1.unpin();

        assertSame(frame1, frame1.requestValidFrame());
        frame1.unpin();

        // force a eviction
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();
        bufferManager.fetchNewPageFrame(partNum).unpin();

        assertFalse(frame1.isValid());

        BufferFrame frame2 = frame1.requestValidFrame();
        assertNotSame(frame1, frame2);
        frame2.readBytes((short) 67, (short) 4, actual);
        frame2.unpin();

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testFreePage() {
        int partNum = diskSpaceManager.allocPart(1);

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame2 = bufferManager.fetchNewPageFrame(partNum);
        Page page3 = bufferManager.fetchNewPage(new DummyLockContext(), partNum);
        BufferFrame frame4 = bufferManager.fetchNewPageFrame(partNum);
        BufferFrame frame5 = bufferManager.fetchNewPageFrame(partNum);

        frame1.unpin();
        frame2.unpin();
        frame4.unpin();
        frame5.unpin();

        bufferManager.freePage(page3);
        try {
            diskSpaceManager.readPage(page3.getPageNum(), new byte[DiskSpaceManager.PAGE_SIZE]);
            fail();
        } catch (PageException e) { /* do nothing */ }

        BufferFrame frame6  = bufferManager.fetchNewPageFrame(partNum);
        frame6.unpin();
        assertTrue(frame1.isValid());
        assertTrue(frame2.isValid());
        assertTrue(frame4.isValid());
        assertTrue(frame5.isValid());
        assertTrue(frame6.isValid());
    }

    @Test
    public void testFreePart() {
        int partNum1 = diskSpaceManager.allocPart(1);
        int partNum2 = diskSpaceManager.allocPart(2);

        BufferFrame frame1 = bufferManager.fetchNewPageFrame(partNum1);
        BufferFrame frame2 = bufferManager.fetchNewPageFrame(partNum2);
        BufferFrame frame3 = bufferManager.fetchNewPageFrame(partNum1);
        BufferFrame frame4 = bufferManager.fetchNewPageFrame(partNum2);
        BufferFrame frame5 = bufferManager.fetchNewPageFrame(partNum2);

        frame1.unpin();
        frame2.unpin();
        frame3.unpin();
        frame4.unpin();
        frame5.unpin();

        bufferManager.freePart(partNum1);

        try {
            diskSpaceManager.readPage(frame1.getPageNum(), new byte[DiskSpaceManager.PAGE_SIZE]);
            fail();
        } catch (Exception e) { /* do nothing */ }
        try {
            diskSpaceManager.readPage(frame3.getPageNum(), new byte[DiskSpaceManager.PAGE_SIZE]);
            fail();
        } catch (Exception e) { /* do nothing */ }
        try {
            diskSpaceManager.allocPage(partNum1);
            fail();
        } catch (Exception e) { /* do nothing */ }

        BufferFrame frame6  = bufferManager.fetchNewPageFrame(partNum2);
        BufferFrame frame7  = bufferManager.fetchNewPageFrame(partNum2);
        frame6.unpin();
        frame7.unpin();
        assertFalse(frame1.isValid());
        assertTrue(frame2.isValid());
        assertFalse(frame3.isValid());
        assertTrue(frame4.isValid());
        assertTrue(frame5.isValid());
        assertTrue(frame6.isValid());
        assertTrue(frame7.isValid());
    }

    @Test(expected = PageException.class)
    public void testMissingPart() {
        bufferManager.fetchPageFrame(DiskSpaceManager.getVirtualPageNum(0, 0));
    }

    @Test(expected = PageException.class)
    public void testMissingPage() {
        int partNum = diskSpaceManager.allocPart(1);
        bufferManager.fetchPageFrame(DiskSpaceManager.getVirtualPageNum(partNum, 0));
    }
}
