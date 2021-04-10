package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestPageDirectory {
    private BufferManager bufferManager;
    private PageDirectory pageDirectory;

    @Before
    public void setup() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        pageDirectory = null;
    }

    @After
    public void cleanup() {
        bufferManager.close();
    }

    private void createPageDirectory(long pageNum, short metadataSize) {
        pageDirectory = new PageDirectory(bufferManager, 0, pageNum, metadataSize, new DummyLockContext());
    }

    private void createPageDirectory(short metadataSize) {
        Page page = bufferManager.fetchNewPage(new DummyLockContext("_dummyPageDirectoryRecord"), 0);
        try {
            createPageDirectory(page.getPageNum(), metadataSize);
        } finally {
            page.unpin();
        }
    }

    @Test
    public void testGetPageWithSpace() {
        createPageDirectory((short) 10);

        short pageSize = (short) (pageDirectory.getEffectivePageSize() - 10);
        Page p1 = pageDirectory.getPageWithSpace(pageSize);
        Page p2 = pageDirectory.getPageWithSpace((short) 1);
        Page p3 = pageDirectory.getPageWithSpace((short) 60);
        Page p4 = pageDirectory.getPageWithSpace((short) (pageSize - 60));
        Page p5 = pageDirectory.getPageWithSpace((short) 120);

        p1.unpin(); p2.unpin(); p3.unpin(); p4.unpin(); p5.unpin();

        assertNotEquals(p1, p2);
        assertEquals(p2, p3);
        assertEquals(p3, p5);
        assertNotEquals(p4, p5);
    }

    @Test
    public void testGetPageWithSpaceInvalid() {
        createPageDirectory((short) 1000);

        short pageSize = (short) (pageDirectory.getEffectivePageSize() - 1000);

        try {
            pageDirectory.getPageWithSpace((short) (pageSize + 1));
            fail();
        } catch (IllegalArgumentException e) { /* do nothing */ }

        try {
            pageDirectory.getPageWithSpace((short) 0);
            fail();
        } catch (IllegalArgumentException e) { /* do nothing */ }

        try {
            pageDirectory.getPageWithSpace((short) - 1);
            fail();
        } catch (IllegalArgumentException e) { /* do nothing */ }
    }

    @Test
    public void testGetPage() {
        createPageDirectory((short) 10);

        short pageSize = (short) (pageDirectory.getEffectivePageSize() - 10);
        Page p1 = pageDirectory.getPageWithSpace(pageSize);
        Page p2 = pageDirectory.getPageWithSpace((short) 1);
        Page p3 = pageDirectory.getPageWithSpace((short) 60);
        Page p4 = pageDirectory.getPageWithSpace((short) (pageSize - 60));
        Page p5 = pageDirectory.getPageWithSpace((short) 120);

        p1.unpin(); p2.unpin(); p3.unpin(); p4.unpin(); p5.unpin();

        Page pp1 = pageDirectory.getPage(p1.getPageNum());
        Page pp2 = pageDirectory.getPage(p2.getPageNum());
        Page pp3 = pageDirectory.getPage(p3.getPageNum());
        Page pp4 = pageDirectory.getPage(p4.getPageNum());
        Page pp5 = pageDirectory.getPage(p5.getPageNum());

        pp1.unpin(); pp2.unpin(); pp3.unpin(); pp4.unpin(); pp5.unpin();

        assertEquals(p1, pp1);
        assertEquals(p2, pp2);
        assertEquals(p3, pp3);
        assertEquals(p4, pp4);
        assertEquals(p5, pp5);
    }

    @Test
    public void testGetPageInvalid() {
        createPageDirectory((short) 10);
        Page p = pageDirectory.getPageWithSpace((short) 1);
        p.unpin();

        createPageDirectory((short) 10);
        try {
            pageDirectory.getPage(p.getPageNum());
            fail();
        } catch (PageException e) { /* do nothing */ }

        try {
            pageDirectory.getPage(DiskSpaceManager.INVALID_PAGE_NUM);
            fail();
        } catch (PageException e) { /* do nothing */ }
    }

    @Test
    public void testUpdateFreeSpace() {
        createPageDirectory((short) 10);

        short pageSize = (short) (pageDirectory.getEffectivePageSize() - 10);
        Page p1 = pageDirectory.getPageWithSpace(pageSize);
        p1.unpin();

        pageDirectory.updateFreeSpace(p1, (short) 10);

        Page p2 = pageDirectory.getPageWithSpace((short) 10);
        p2.unpin();

        assertEquals(p1, p2);
    }

    @Test
    public void testUpdateFreeSpaceInvalid1() {
        createPageDirectory((short) 10);

        short pageSize = (short) (pageDirectory.getEffectivePageSize() - 10);
        Page p1 = pageDirectory.getPageWithSpace(pageSize);
        p1.unpin();

        pageDirectory.updateFreeSpace(p1, pageSize);
        try {
            pageDirectory.updateFreeSpace(p1, (short) 10);
            fail();
        } catch (PageException e) { /* do nothing */ }
    }

    @Test
    public void testUpdateFreeSpaceInvalid2() {
        createPageDirectory((short) 10);

        short pageSize = (short) (pageDirectory.getEffectivePageSize() - 10);
        Page p1 = pageDirectory.getPageWithSpace(pageSize);
        p1.unpin();

        try {
            pageDirectory.updateFreeSpace(p1, (short) - 1);
            fail();
        } catch (IllegalArgumentException e) { /* do nothing */ }

        try {
            pageDirectory.updateFreeSpace(p1, (short) (pageSize + 1));
            fail();
        } catch (IllegalArgumentException e) { /* do nothing */ }
    }

    @Test
    public void testIterator() {
        createPageDirectory((short) 0);
        createPageDirectory((short) (pageDirectory.getEffectivePageSize() - 30));

        int numRequests = 100;
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < numRequests; ++i) {
            Page page = pageDirectory.getPageWithSpace((short) 13);
            if (pages.size() == 0 || !pages.get(pages.size() - 1).equals(page)) {
                pages.add(page);
            }
            page.unpin();
        }

        Iterator<Page> iter = pageDirectory.iterator();
        for (Page page : pages) {
            assertTrue(iter.hasNext());

            Page p = iter.next();
            p.unpin();
            assertEquals(page, p);
        }
    }

    @Test
    public void testIteratorWithDeletes() {
        createPageDirectory((short) 0);
        createPageDirectory((short) (pageDirectory.getEffectivePageSize() - 30));

        int numRequests = 100;
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < numRequests; ++i) {
            Page page = pageDirectory.getPageWithSpace((short) 13);
            if (pages.size() == 0 || !pages.get(pages.size() - 1).equals(page)) {
                pages.add(page);
            }
            page.unpin();
        }

        Iterator<Page> iterator = pages.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            if (iterator.hasNext()) {
                pageDirectory.updateFreeSpace(iterator.next(), (short) 30);
                iterator.remove();
            }
        }

        Iterator<Page> iter = pageDirectory.iterator();
        for (Page page : pages) {
            assertTrue(iter.hasNext());

            Page p = iter.next();
            p.unpin();
            assertEquals(page, p);
        }
    }
}
