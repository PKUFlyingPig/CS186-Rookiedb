package edu.berkeley.cs186.database.io;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of a disk space manager with virtual page translation, and
 * two levels of header pages, allowing for (with page size of 4K) 256G worth of data per partition:
 *
 *                                           [master page]
 *                  /                              |                               \
 *           [header page]                                                    [header page]
 *     /    |     |     |     \                   ...                   /    |     |     |     \
 * [data] [data] ... [data] [data]                                   [data] [data] ... [data] [data]
 *
 * Each header page stores a bitmap, indicating whether each of the data pages has been allocated,
 * and manages 32K pages. The master page stores 16-bit integers for each of the header pages indicating
 * the number of data pages that have been allocated under the header page (managing 2K header pages).
 * A single partition may therefore have a maximum of 64M data pages.
 *
 * Master and header pages are cached permanently in memory; changes to these are immediately flushed to
 * disk. This imposes a fairly small memory overhead (128M partitions have 2 pages cached). This caching
 * is done separately from the buffer manager's caching.
 *
 * Virtual page numbers are 64-bit integers (Java longs) assigned to data pages in the following format:
 *       partition number * 10^10 + n
 * for the n-th data page of the partition (indexed from 0). This particular format (instead of a simpler
 * scheme such as assigning the upper 32 bits to partition number and lower 32 to page number) was chosen
 * for ease of debugging (it's easier to read 10000000006 as part 1 page 6, than it is to decipher 4294967302).
 *
 * Partitions are backed by OS level files (one OS level file per partition), and are stored in the following
 * manner:
 * - the master page is the 0th page of the OS file
 * - the first header page is the 1st page of the OS file
 * - the next 32K pages are data pages managed by the first header page
 * - the second header page follows
 * - the next 32K pages are data pages managed by the second header page
 * - etc.
 */
public class DiskSpaceManagerImpl implements DiskSpaceManager {
    static final int MAX_HEADER_PAGES = PAGE_SIZE / 2; // 2 bytes per header page
    static final int DATA_PAGES_PER_HEADER = PAGE_SIZE * 8; // 1 bit per data page

    // Name of base directory.
    private String dbDir;

    // Info about each partition.
    private Map<Integer, PartitionHandle> partInfo;

    // Counter to generate new partition numbers.
    private AtomicInteger partNumCounter;

    // Lock on the entire manager.
    private ReentrantLock managerLock;

    // recovery manager
    private RecoveryManager recoveryManager;

    /**
     * Initialize the disk space manager using the given directory. Creates the directory
     * if not present.
     *
     * @param dbDir base directory of the database
     */
    public DiskSpaceManagerImpl(String dbDir, RecoveryManager recoveryManager) {
        this.dbDir = dbDir;
        this.recoveryManager = recoveryManager;
        this.partInfo = new HashMap<>();
        this.partNumCounter = new AtomicInteger(0);
        this.managerLock = new ReentrantLock();

        File dir = new File(dbDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new PageException("could not initialize disk space manager - could not make directory");
            }
        } else {
            int maxFileNum = -1;
            File[] files = dir.listFiles();
            if (files == null) {
                throw new PageException("could not initialize disk space manager - directory is a file");
            }
            for (File f : files) {
                if (f.length() == 0) {
                    if (!f.delete()) {
                        throw new PageException("could not clean up unused file - " + f.getName());
                    }
                    continue;
                }
                int fileNum = Integer.parseInt(f.getName());
                maxFileNum = Math.max(maxFileNum, fileNum);

                PartitionHandle pi = new PartitionHandle(fileNum, recoveryManager);
                pi.open(dbDir + "/" + f.getName());
                this.partInfo.put(fileNum, pi);
            }
            this.partNumCounter.set(maxFileNum + 1);
        }
    }

    @Override
    public void close() {
        for (Map.Entry<Integer, PartitionHandle> part : this.partInfo.entrySet()) {
            try {
                part.getValue().close();
            } catch (IOException e) {
                throw new PageException("could not close partition " + part.getKey() + ": " + e.getMessage());
            }
        }
    }

    @Override
    public int allocPart() {
        return this.allocPartHelper(this.partNumCounter.getAndIncrement());
    }

    @Override
    public int allocPart(int partNum) {
        this.partNumCounter.updateAndGet((int x) -> Math.max(x, partNum) + 1);
        return this.allocPartHelper(partNum);
    }

    private int allocPartHelper(int partNum) {
        PartitionHandle pi;

        this.managerLock.lock();
        try {
            if (this.partInfo.containsKey(partNum)) {
                throw new IllegalStateException("partition number " + partNum + " already exists");
            }

            pi = new PartitionHandle(partNum, recoveryManager);
            this.partInfo.put(partNum, pi);

            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            // We must open partition only after logging, but we need to release the
            // manager lock first, in case the log manager is currently in the process
            // of allocating a new log page (for another txn's records).
            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                recoveryManager.logAllocPart(transaction.getTransNum(), partNum);
            }

            pi.open(dbDir + "/" + partNum);
            return partNum;
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void freePart(int partNum) {
        PartitionHandle pi;

        this.managerLock.lock();
        try {
            pi = this.partInfo.remove(partNum);
            if (pi == null) {
                throw new NoSuchElementException("no partition " + partNum);
            }
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            try {
                pi.freeDataPages();
                pi.close();
            } catch (IOException e) {
                throw new PageException("could not close partition " + partNum + ": " + e.getMessage());
            }

            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                recoveryManager.logFreePart(transaction.getTransNum(), partNum);
            }

            File pf = new File(dbDir + "/" + partNum);
            if (!pf.delete()) {
                throw new PageException("could not delete files for partition " + partNum);
            }
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public long allocPage(int partNum) {
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            int pageNum = pi.allocPage();
            pi.writePage(pageNum, new byte[PAGE_SIZE]);
            return DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public long allocPage(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        int headerIndex = pageNum / DATA_PAGES_PER_HEADER;
        int pageIndex = pageNum % DATA_PAGES_PER_HEADER;

        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.allocPage(headerIndex, pageIndex);
            pi.writePage(pageNum, new byte[PAGE_SIZE]);
            return DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void freePage(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.freePage(pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void readPage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("readPage expects a page-sized buffer");
        }
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.readPage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not read partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void writePage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("writePage expects a page-sized buffer");
        }
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.writePage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not write partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public boolean pageAllocated(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            return !pi.isNotAllocatedPage(pageNum);
        } finally {
            pi.partitionLock.unlock();
        }
    }

    // Gets PartInfo, throws exception if not found.
    private PartitionHandle getPartInfo(int partNum) {
        PartitionHandle pi = this.partInfo.get(partNum);
        if (pi == null) {
            throw new NoSuchElementException("no partition " + partNum);
        }
        return pi;
    }
}
