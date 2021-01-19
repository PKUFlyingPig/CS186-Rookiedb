package edu.berkeley.cs186.database.io;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestDiskSpaceManager {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private DiskSpaceManager diskSpaceManager;
    private Path managerRoot;

    @Before
    public void beforeEach() throws IOException {
        managerRoot = tempFolder.newFolder("dsm-test").toPath();
    }

    private DiskSpaceManager getDiskSpaceManager() {
        return new DiskSpaceManagerImpl(managerRoot.toString(), new DummyRecoveryManager());
    }

    @Test
    public void testCreateDiskSpaceManager() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.close();
    }

    @Test
    public void testAllocPart() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart(0);

        assertEquals(0, partNum);
        assertTrue(managerRoot.resolve("0").toFile().exists());
        assertEquals(DiskSpaceManager.PAGE_SIZE, managerRoot.resolve("0").toFile().length());

        partNum = diskSpaceManager.allocPart();

        assertEquals(1, partNum);
        assertTrue(managerRoot.resolve("1").toFile().exists());
        assertEquals(DiskSpaceManager.PAGE_SIZE, managerRoot.resolve("1").toFile().length());

        diskSpaceManager.close();
    }

    @Test
    public void testAllocPartPersist() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.allocPart();
        diskSpaceManager.close();
        diskSpaceManager = getDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart();
        diskSpaceManager.close();

        assertEquals(1, partNum);
        assertTrue(managerRoot.resolve("1").toFile().exists());
        assertEquals(DiskSpaceManager.PAGE_SIZE, managerRoot.resolve("0").toFile().length());
    }

    @Test(expected = NoSuchElementException.class)
    public void testFreePartBad() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.freePart(1);
        diskSpaceManager.close();
    }

    @Test
    public void testFreePart() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart();
        diskSpaceManager.freePart(partNum);

        assertFalse(managerRoot.resolve("0").toFile().exists());

        diskSpaceManager.close();
    }

    @Test
    public void testFreePartPersist() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart();
        diskSpaceManager.freePart(partNum);
        diskSpaceManager.close();
        diskSpaceManager = getDiskSpaceManager();
        partNum = diskSpaceManager.allocPart();
        diskSpaceManager.close();

        assertEquals(0, partNum);
        assertTrue(managerRoot.resolve("0").toFile().exists());
        assertFalse(managerRoot.resolve("1").toFile().exists());
    }

    @Test
    public void testAllocPageZeroed() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart(0);
        long pageNum1 = diskSpaceManager.allocPage(0);
        long pageNum2 = diskSpaceManager.allocPage(partNum);

        assertEquals(0L, pageNum1);
        assertEquals(1L, pageNum2);

        byte[] buf = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum1, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        diskSpaceManager.readPage(pageNum2, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);

        long pageNum3 = diskSpaceManager.allocPage(partNum);
        long pageNum4 = diskSpaceManager.allocPage(partNum);
        diskSpaceManager.close();

        diskSpaceManager = getDiskSpaceManager();

        diskSpaceManager.readPage(pageNum1, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        diskSpaceManager.readPage(pageNum2, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        diskSpaceManager.readPage(pageNum3, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);
        diskSpaceManager.readPage(pageNum4, buf);
        assertArrayEquals(new byte[DiskSpaceManager.PAGE_SIZE], buf);

        diskSpaceManager.close();
    }

    @Test(expected = NoSuchElementException.class)
    public void testReadBadPart() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.readPage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        diskSpaceManager.close();
    }

    @Test(expected = NoSuchElementException.class)
    public void testWriteBadPart() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.writePage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        diskSpaceManager.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadBadBuffer() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.readPage(0, new byte[DiskSpaceManager.PAGE_SIZE - 1]);
        diskSpaceManager.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteBadBuffer() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.writePage(0, new byte[DiskSpaceManager.PAGE_SIZE + 1]);
        diskSpaceManager.close();
    }

    @Test(expected = PageException.class)
    public void testReadOutOfBounds() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.allocPart();
        diskSpaceManager.readPage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        diskSpaceManager.close();
    }

    @Test(expected = PageException.class)
    public void testWriteOutOfBounds() {
        diskSpaceManager = getDiskSpaceManager();
        diskSpaceManager.allocPart();
        diskSpaceManager.writePage(0, new byte[DiskSpaceManager.PAGE_SIZE]);
        diskSpaceManager.close();
    }

    @Test
    public void testReadWrite() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart();
        long pageNum = diskSpaceManager.allocPage(partNum);

        byte[] buf = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
        }
        diskSpaceManager.writePage(pageNum, buf);
        byte[] readbuf = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum, readbuf);

        assertArrayEquals(buf, readbuf);

        diskSpaceManager.freePart(partNum);
        diskSpaceManager.close();
    }

    @Test
    public void testReadWritePersistent() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum = diskSpaceManager.allocPart();
        long pageNum = diskSpaceManager.allocPage(partNum);

        byte[] buf = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
        }
        diskSpaceManager.writePage(pageNum, buf);
        diskSpaceManager.close();

        diskSpaceManager = getDiskSpaceManager();
        byte[] readbuf = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum, readbuf);

        assertArrayEquals(buf, readbuf);

        diskSpaceManager.freePart(partNum);
        diskSpaceManager.close();
    }

    @Test
    public void testReadWriteMultiplePartitions() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum1 = diskSpaceManager.allocPart();
        int partNum2 = diskSpaceManager.allocPart();
        long pageNum11 = diskSpaceManager.allocPage(partNum1);
        long pageNum21 = diskSpaceManager.allocPage(partNum2);
        long pageNum22 = diskSpaceManager.allocPage(partNum2);

        byte[] buf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf1.length; ++i) {
            buf1[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
            buf2[i] = (byte) ((Integer.valueOf(i).hashCode() >> 8) & 0xFF);
            buf3[i] = (byte) ((Integer.valueOf(i).hashCode() >> 16) & 0xFF);
        }
        diskSpaceManager.writePage(pageNum11, buf1);
        diskSpaceManager.writePage(pageNum22, buf3);
        diskSpaceManager.writePage(pageNum21, buf2);
        byte[] readbuf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum11, readbuf1);
        diskSpaceManager.readPage(pageNum21, readbuf2);
        diskSpaceManager.readPage(pageNum22, readbuf3);

        assertArrayEquals(buf1, readbuf1);
        assertArrayEquals(buf2, readbuf2);
        assertArrayEquals(buf3, readbuf3);

        diskSpaceManager.freePart(partNum1);
        diskSpaceManager.freePart(partNum2);
        diskSpaceManager.close();
    }

    @Test
    public void testReadWriteMultiplePartitionsPersistent() {
        diskSpaceManager = getDiskSpaceManager();
        int partNum1 = diskSpaceManager.allocPart();
        int partNum2 = diskSpaceManager.allocPart();
        long pageNum11 = diskSpaceManager.allocPage(partNum1);
        long pageNum21 = diskSpaceManager.allocPage(partNum2);
        long pageNum22 = diskSpaceManager.allocPage(partNum2);

        byte[] buf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] buf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        for (int i = 0; i < buf1.length; ++i) {
            buf1[i] = (byte) (Integer.valueOf(i).hashCode() & 0xFF);
            buf2[i] = (byte) ((Integer.valueOf(i).hashCode() >> 8) & 0xFF);
            buf3[i] = (byte) ((Integer.valueOf(i).hashCode() >> 16) & 0xFF);
        }
        diskSpaceManager.writePage(pageNum11, buf1);
        diskSpaceManager.writePage(pageNum22, buf3);
        diskSpaceManager.writePage(pageNum21, buf2);
        diskSpaceManager.close();

        diskSpaceManager = getDiskSpaceManager();
        byte[] readbuf1 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf2 = new byte[DiskSpaceManager.PAGE_SIZE];
        byte[] readbuf3 = new byte[DiskSpaceManager.PAGE_SIZE];
        diskSpaceManager.readPage(pageNum11, readbuf1);
        diskSpaceManager.readPage(pageNum21, readbuf2);
        diskSpaceManager.readPage(pageNum22, readbuf3);

        assertArrayEquals(buf1, readbuf1);
        assertArrayEquals(buf2, readbuf2);
        assertArrayEquals(buf3, readbuf3);

        diskSpaceManager.freePart(partNum1);
        diskSpaceManager.freePart(partNum2);
        diskSpaceManager.close();
    }
}

