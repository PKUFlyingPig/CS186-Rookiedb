package edu.berkeley.cs186.database.io;

import java.util.*;

/**
 * "Disk" space manager that really just keeps things in memory. Not thread safe.
 */
public class MemoryDiskSpaceManager implements DiskSpaceManager {
    private Map<Integer, Set<Integer>> partitions = new HashMap<>();
    private Map<Integer, Integer> nextPageNum = new HashMap<>();
    private Map<Long, byte[]> pages = new HashMap<>();
    private int nextPartitionNum = 0;

    @Override
    public void close() {}

    @Override
    public int allocPart() {
        partitions.put(nextPartitionNum, new HashSet<>());
        nextPageNum.put(nextPartitionNum, 0);
        return nextPartitionNum++;
    }

    @Override
    public int allocPart(int partNum) {
        if (partitions.containsKey(partNum)) {
            throw new IllegalStateException("partition " + partNum + " already allocated");
        }
        partitions.put(partNum, new HashSet<>());
        nextPageNum.put(partNum, 0);
        nextPartitionNum = partNum + 1;
        return partNum;
    }

    @Override
    public void freePart(int partNum) {
        if (!partitions.containsKey(partNum)) {
            throw new NoSuchElementException("partition " + partNum + " not allocated");
        }
        for (Integer pageNum : partitions.remove(partNum)) {
            pages.remove(DiskSpaceManager.getVirtualPageNum(partNum, pageNum));
        }
        nextPageNum.remove(partNum);
    }

    @Override
    public long allocPage(int partNum) {
        if (!partitions.containsKey(partNum)) {
            throw new IllegalArgumentException("partition " + partNum + " not allocated");
        }
        int ppageNum = nextPageNum.get(partNum);
        nextPageNum.put(partNum, ppageNum + 1);
        long pageNum = DiskSpaceManager.getVirtualPageNum(partNum, ppageNum);
        partitions.get(partNum).add(ppageNum);
        pages.put(pageNum, new byte[DiskSpaceManager.PAGE_SIZE]);
        return pageNum;
    }

    @Override
    public long allocPage(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int ppageNum = DiskSpaceManager.getPageNum(page);
        if (!partitions.containsKey(partNum)) {
            throw new IllegalArgumentException("partition " + partNum + " not allocated");
        }
        if (pages.containsKey(page)) {
            throw new IllegalStateException("page " + page + " already allocated");
        }
        nextPageNum.put(partNum, ppageNum + 1);
        partitions.get(partNum).add(ppageNum);
        pages.put(page, new byte[DiskSpaceManager.PAGE_SIZE]);
        return page;
    }

    @Override
    public void freePage(long page) {
        if (!pages.containsKey(page)) {
            throw new NoSuchElementException("page " + page + " not allocated");
        }
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        partitions.get(partNum).remove(pageNum);
        pages.remove(page);
    }

    @Override
    public void readPage(long page, byte[] buf) {
        if (buf.length != DiskSpaceManager.PAGE_SIZE) {
            throw new IllegalArgumentException("bad buffer size");
        }
        if (!pages.containsKey(page)) {
            throw new PageException("page " + page + " not allocated");
        }
        System.arraycopy(pages.get(page), 0, buf, 0, DiskSpaceManager.PAGE_SIZE);
    }

    @Override
    public void writePage(long page, byte[] buf) {
        if (buf.length != DiskSpaceManager.PAGE_SIZE) {
            throw new IllegalArgumentException("bad buffer size");
        }
        if (!pages.containsKey(page)) {
            throw new PageException("page " + page + " not allocated");
        }
        System.arraycopy(buf, 0, pages.get(page), 0, DiskSpaceManager.PAGE_SIZE);
    }

    @Override
    public boolean pageAllocated(long page) {
        return pages.containsKey(page);
    }
}
