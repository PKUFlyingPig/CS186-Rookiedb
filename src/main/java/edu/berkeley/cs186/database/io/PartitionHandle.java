package edu.berkeley.cs186.database.io;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Bits;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

import static edu.berkeley.cs186.database.io.DiskSpaceManager.PAGE_SIZE;
import static edu.berkeley.cs186.database.io.DiskSpaceManagerImpl.DATA_PAGES_PER_HEADER;
import static edu.berkeley.cs186.database.io.DiskSpaceManagerImpl.MAX_HEADER_PAGES;

class PartitionHandle implements AutoCloseable {
    // Lock on the partition.
    ReentrantLock partitionLock;

    // Underlying OS file/file channel.
    private RandomAccessFile file;
    private FileChannel fileChannel;

    // Contents of the master page of this partition
    // Ideally would be an unsigned short array but Java doesn't have unsigned types
    private int[] masterPage;

    // Contents of the various header pages of this partition
    private byte[][] headerPages;

    // Recovery manager
    private RecoveryManager recoveryManager;

    // Partition number
    private int partNum;

    PartitionHandle(int partNum, RecoveryManager recoveryManager) {
        this.masterPage = new int[MAX_HEADER_PAGES];
        this.headerPages = new byte[MAX_HEADER_PAGES][];
        this.partitionLock = new ReentrantLock();
        this.recoveryManager = recoveryManager;
        this.partNum = partNum;
    }

    /**
     * Opens the OS file and loads master and header pages.
     * @param fileName name of OS file partition is stored in
     */
    void open(String fileName) {
        assert (this.fileChannel == null);
        try {
            this.file = new RandomAccessFile(fileName, "rw");
            this.fileChannel = this.file.getChannel();
            long length = this.file.length();
            if (length == 0) {
                // new file, write empty master page
                this.writeMasterPage();
            } else {
                // old file, read in master page + header pages
                ByteBuffer b = ByteBuffer.wrap(new byte[PAGE_SIZE]);
                this.fileChannel.read(b, PartitionHandle.masterPageOffset());
                b.position(0);
                for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
                    this.masterPage[i] = Short.toUnsignedInt(b.getShort());
                    if (PartitionHandle.headerPageOffset(i) < length) {
                        // Load header pages that were already in the file
                        byte[] headerPage = new byte[PAGE_SIZE];
                        this.headerPages[i] = headerPage;
                        this.fileChannel.read(ByteBuffer.wrap(headerPage), PartitionHandle.headerPageOffset(i));
                    }
                }
            }
        } catch (IOException e) {
            throw new PageException("Could not open or read file: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        this.partitionLock.lock();
        try {
            Arrays.fill(this.headerPages, null);
            this.file.close();
            this.fileChannel.close();
        } finally {
            this.partitionLock.unlock();
        }
    }

    /**
     * Writes the master page to disk.
     */
    private void writeMasterPage() throws IOException {
        ByteBuffer b = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
            b.putShort((short) masterPage[i]);
        }
        b.position(0);
        this.fileChannel.write(b, PartitionHandle.masterPageOffset());
    }

    /**
     * Writes a header page to disk.
     * @param headerIndex which header page
     */
    private void writeHeaderPage(int headerIndex) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(this.headerPages[headerIndex]);
        this.fileChannel.write(b, PartitionHandle.headerPageOffset(headerIndex));
    }

    /**
     * Allocates a new page in the partition.
     * @return data page number
     */
    int allocPage() throws IOException {
        int headerIndex = -1;
        for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
            if (this.masterPage[i] < DATA_PAGES_PER_HEADER) {
                headerIndex = i;
                break;
            }
        }
        if (headerIndex == -1) {
            throw new PageException("no free pages - partition has reached max size");
        }

        byte[] headerBytes = this.headerPages[headerIndex];

        int pageIndex = -1;
        if (headerBytes == null) {
            pageIndex = 0;
        } else {
            for (int i = 0; i < DATA_PAGES_PER_HEADER; i++) {
                if (Bits.getBit(headerBytes, i) == Bits.Bit.ZERO) {
                    pageIndex = i;
                    break;
                }
            }
            if (pageIndex == -1) {
                throw new PageException("header page should have free space, but doesn't");
            }
        }

        return this.allocPage(headerIndex, pageIndex);
    }

    /**
     * Allocates a new page in the partition.
     * @param headerIndex index of header page managing new page
     * @param pageIndex index within header page of new page
     * @return data page number
     */
    int allocPage(int headerIndex, int pageIndex) throws IOException {
        byte[] headerBytes = this.headerPages[headerIndex];
        if (headerBytes == null) {
            headerBytes = new byte[PAGE_SIZE];
            this.headerPages[headerIndex] = headerBytes;
        }

        if (Bits.getBit(headerBytes, pageIndex) == Bits.Bit.ONE) {
            throw new IllegalStateException("page at (part=" + partNum + ", header=" + headerIndex + ", index="
                                            +
                                            pageIndex + ") already allocated");
        }

        Bits.setBit(headerBytes, pageIndex, Bits.Bit.ONE);
        this.masterPage[headerIndex] = Bits.countBits(headerBytes);

        int pageNum = pageIndex + headerIndex * DATA_PAGES_PER_HEADER;

        TransactionContext transaction = TransactionContext.getTransaction();
        long vpn = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        if (transaction != null) {
            recoveryManager.logAllocPage(transaction.getTransNum(), vpn);
        }
        recoveryManager.diskIOHook(vpn);
        this.writeMasterPage();
        this.writeHeaderPage(headerIndex);

        return pageNum;
    }

    /**
     * Frees a page in the partition from use.
     * @param pageNum data page number to be freed
     */
    void freePage(int pageNum) throws IOException {
        int headerIndex = pageNum / DATA_PAGES_PER_HEADER;
        int pageIndex = pageNum % DATA_PAGES_PER_HEADER;

        byte[] headerBytes = headerPages[headerIndex];
        if (headerBytes == null) {
            throw new NoSuchElementException("cannot free unallocated page");
        }

        if (Bits.getBit(headerBytes, pageIndex) == Bits.Bit.ZERO) {
            throw new NoSuchElementException("cannot free unallocated page");
        }

        TransactionContext transaction = TransactionContext.getTransaction();
        long vpn = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        if (transaction != null) {
            byte[] contents = new byte[PAGE_SIZE];
            readPage(pageNum, contents);
            int halfway = BufferManager.RESERVED_SPACE + BufferManager.EFFECTIVE_PAGE_SIZE / 2;
            recoveryManager.logPageWrite(
                    transaction.getTransNum(),
                    vpn,
                    (short) 0,
                    Arrays.copyOfRange(contents, BufferManager.RESERVED_SPACE, halfway),
                    new byte[BufferManager.EFFECTIVE_PAGE_SIZE / 2]
            );
            recoveryManager.logPageWrite(
                    transaction.getTransNum(),
                    vpn,
                    (short) (BufferManager.EFFECTIVE_PAGE_SIZE / 2),
                    Arrays.copyOfRange(contents, halfway, PAGE_SIZE),
                    new byte[BufferManager.EFFECTIVE_PAGE_SIZE / 2]
            );
            recoveryManager.logFreePage(transaction.getTransNum(), vpn);
        }
        recoveryManager.diskIOHook(vpn);
        Bits.setBit(headerBytes, pageIndex, Bits.Bit.ZERO);
        this.masterPage[headerIndex] = Bits.countBits(headerBytes);
        this.writeMasterPage();
        this.writeHeaderPage(headerIndex);
    }

    /**
     * Reads in a data page. Assumes that the partition lock is held.
     * @param pageNum data page number to read in
     * @param buf output buffer to be filled with page - assumed to be page size
     */
    void readPage(int pageNum, byte[] buf) throws IOException {
        if (this.isNotAllocatedPage(pageNum)) {
            throw new PageException("page " + pageNum + " is not allocated");
        }
        ByteBuffer b = ByteBuffer.wrap(buf);
        this.fileChannel.read(b, PartitionHandle.dataPageOffset(pageNum));
    }

    /**
     * Writes to a data page. Assumes that the partition lock is held.
     * @param pageNum data page number to write to
     * @param buf input buffer with new contents of page - assumed to be page size
     */
    void writePage(int pageNum, byte[] buf) throws IOException {
        if (this.isNotAllocatedPage(pageNum)) {
            throw new PageException("page " + pageNum + " is not allocated");
        }
        ByteBuffer b = ByteBuffer.wrap(buf);
        this.fileChannel.write(b, PartitionHandle.dataPageOffset(pageNum));
        this.fileChannel.force(false);

        long vpn = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        recoveryManager.diskIOHook(vpn);
    }

    /**
     * Checks if page number is for an unallocated data page
     * @param pageNum data page number
     * @return true if page is not valid or not allocated
     */
    boolean isNotAllocatedPage(int pageNum) {
        int headerIndex = pageNum / DATA_PAGES_PER_HEADER;
        int pageIndex = pageNum % DATA_PAGES_PER_HEADER;
        if (headerIndex < 0 || headerIndex >= MAX_HEADER_PAGES) {
            return true;
        }
        if (masterPage[headerIndex] == 0) {
            return true;
        }
        return Bits.getBit(headerPages[headerIndex], pageIndex) == Bits.Bit.ZERO;
    }

    /**
     * Frees all data pages from partition for use
     * @throws IOException
     */
    void freeDataPages() throws IOException {
        for (int i = 0; i < MAX_HEADER_PAGES; ++i) {
            if (masterPage[i] > 0) {
                byte[] headerPage = headerPages[i];
                for (int j = 0; j < DATA_PAGES_PER_HEADER; ++j) {
                    if (Bits.getBit(headerPage, j) == Bits.Bit.ONE) {
                        this.freePage(i * DATA_PAGES_PER_HEADER + j);
                    }
                }
            }
        }
    }

    /**
     * @return offset in OS file for master page
     */
    private static long masterPageOffset() {
        return 0;
    }

    /**
     * @param headerIndex which header page
     * @return offset in OS file for header page
     */
    private static long headerPageOffset(int headerIndex) {
        // Consider the layout if we had 4 data pages per header:
        // Offset (in pages):  0  1  2  3  4  5  6  7  8  9 10 11
        // Page Type:         [M][H][D][D][D][D][H][D][D][D][D][H]...
        // Header Index:          0              1              2
        // To get the offset in pages of a header page you should add 1 for
        // the master page, and then take the header index times the number
        // of data pages per header plus 1 to account for the header page
        // itself (in the above example this coefficient would be 5)
        long spacingCoeff = DATA_PAGES_PER_HEADER + 1; // Promote to long
        return (1 + headerIndex * spacingCoeff) * PAGE_SIZE;
    }

    /**
     * @param pageNum data page number
     * @return offset in OS file for data page
     */
    private static long dataPageOffset(int pageNum) {
        // Consider the layout if we had 4 data pages per header:
        // Offset (in pages):  0  1  2  3  4  5  6  7  8  9 10
        // Page Type:         [M][H][D][D][D][D][H][D][D][D][D]
        // Data Page Index:          0  1  2  3     4  5  6  7
        // To get the offset in pages of a given data page you should:
        // - add one for the master page
        // - add one for the first header page
        // - add how many other header pages precede the data page
        //   (found by floor dividing page num by data pages per header)
        // - add how many data pages precede the given data page
        //   (this works out conveniently to the page's page number)
        long otherHeaders = pageNum / DATA_PAGES_PER_HEADER;
        return (2 + otherHeaders + pageNum) * PAGE_SIZE;
    }
}