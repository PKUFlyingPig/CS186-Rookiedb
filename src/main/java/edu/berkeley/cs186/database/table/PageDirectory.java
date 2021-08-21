package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterable;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.ConcatBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.IndexBacktrackingIterator;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.NoSuchElementException;
import java.util.Random;

/**
 * An implementation of a heap file, using a page directory. Assumes data pages are packed (but record
 * lengths do not need to be fixed-length).
 *
 * Header pages are layed out as follows:
 * - first byte: 0x1 to indicate valid allocated page
 * - next 4 bytes: page directory id
 * - next 8 bytes: page number of next header page, or -1 (0xFFFFFFFFFFFFFFFF) if no next header page.
 * - next 10 bytes: page number of data page (or -1), followed by 2 bytes of amount of free space
 * - repeat 10 byte entries
 *
 * Data pages contain a small header containing:
 * - 4-byte page directory id
 * - 4-byte index of which header page manages it
 * - 2-byte offset indicating which slot in the header page its data page entry resides
 *
 * This header is used to quickly locate and update the header page when the amount of free space on the data page
 * changes, as well as ensure that we do not modify pages in other page directories by accident.
 *
 * The page directory id is a randomly generated 32-bit integer used to help detect bugs (where we attempt
 * to write to a page that is not managed by the page directory).
 */
public class PageDirectory implements BacktrackingIterable<Page> {
    // size of the header in header pages
    private static final short HEADER_HEADER_SIZE = 13;

    // number of data page entries in a header page
    private static final short HEADER_ENTRY_COUNT = (BufferManager.EFFECTIVE_PAGE_SIZE -
            HEADER_HEADER_SIZE) / DataPageEntry.SIZE;

    // size of the header in data pages
    private static final short DATA_HEADER_SIZE = 10;

    // effective page size
    public static final short EFFECTIVE_PAGE_SIZE = BufferManager.EFFECTIVE_PAGE_SIZE -
            DATA_HEADER_SIZE;

    // the buffer manager
    private BufferManager bufferManager;

    // partition to allocate new header pages in - may be different from partition
    // for data pages
    private int partNum;

    // First header page
    private HeaderPage firstHeader;

    // Size of metadata of an empty data page.
    private short emptyPageMetadataSize;

    // lock context of heap file/table
    private LockContext lockContext;

    // page directory id
    private int pageDirectoryId;

    /**
     * Creates a new heap file, or loads existing file if one already
     * exists at partNum.
     * @param bufferManager buffer manager
     * @param partNum partition to allocate new header pages in (can be different partition
     *                from data pages)
     * @param pageNum first header page of heap file
     * @param emptyPageMetadataSize size of metadata on an empty page
     * @param lockContext lock context of this heap file
     */
    public PageDirectory(BufferManager bufferManager, int partNum, long pageNum,
                         short emptyPageMetadataSize, LockContext lockContext) {
        this.bufferManager = bufferManager;
        this.partNum = partNum;
        this.emptyPageMetadataSize = emptyPageMetadataSize;
        this.lockContext = lockContext;
        this.firstHeader = new HeaderPage(pageNum, 0, true);
    }

    public short getEffectivePageSize() {
        return EFFECTIVE_PAGE_SIZE;
    }

    public void setEmptyPageMetadataSize(short emptyPageMetadataSize) {
        this.emptyPageMetadataSize = emptyPageMetadataSize;
    }

    public Page getPage(long pageNum) {
        return new DataPage(pageDirectoryId, this.bufferManager.fetchPage(lockContext, pageNum));
    }

    public Page getPageWithSpace(short requiredSpace) {
        if (requiredSpace <= 0) {
            throw new IllegalArgumentException("cannot request nonpositive amount of space");
        }
        if (requiredSpace > EFFECTIVE_PAGE_SIZE - emptyPageMetadataSize) {
            throw new IllegalArgumentException("requesting page with more space than the size of the page");
        }

        Page page = this.firstHeader.loadPageWithSpace(requiredSpace);
        LockContext pageContext = lockContext.childContext(page.getPageNum());
        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(pageContext, LockType.X);

        return new DataPage(pageDirectoryId, page);
    }

    public void updateFreeSpace(Page page, short newFreeSpace) {
        if (newFreeSpace <= 0 || newFreeSpace > EFFECTIVE_PAGE_SIZE - emptyPageMetadataSize) {
            throw new IllegalArgumentException("bad size for data page free space");
        }

        int headerIndex;
        short offset;
        page.pin();
        try {
            Buffer b = ((DataPage) page).getFullBuffer();
            b.position(4); // skip page directory id
            headerIndex = b.getInt();
            offset = b.getShort();
        } finally {
            page.unpin();
        }

        HeaderPage headerPage = firstHeader;
        for (int i = 0; i < headerIndex; ++i) {
            headerPage = headerPage.nextPage;
        }
        headerPage.updateSpace(page, offset, newFreeSpace);
    }

    @Override
    public BacktrackingIterator<Page> iterator() {
        return new ConcatBacktrackingIterator<>(new HeaderPageIterator());
    }

    public int getNumDataPages() {
        int numDataPages = 0;
        HeaderPage headerPage = firstHeader;
        while (headerPage != null) {
            numDataPages += headerPage.numDataPages;
            headerPage = headerPage.nextPage;
        }
        return numDataPages;
    }

    public int getPartNum() {
        return partNum;
    }

    /**
     * Wrapper around page object to skip the header and verify that it belongs to this
     * page directory.
     */
    private static class DataPage extends Page {
        private DataPage(int pageDirectoryId, Page page) {
            super(page);

            Buffer buffer = super.getBuffer();
            if (buffer.getInt() != pageDirectoryId) {
                page.unpin();
                throw new PageException("data page directory id does not match");
            }
        }

        @Override
        public Buffer getBuffer() {
            return super.getBuffer().position(DATA_HEADER_SIZE).slice();
        }

        // get the full buffer (without skipping header) for internal use
        private Buffer getFullBuffer() {
            return super.getBuffer();
        }
    }

    /**
     * Entry for a data page inside a header page.
     */
    private static class DataPageEntry {
        // size in bytes of entry
        private static final int SIZE = 10;

        // page number of data page
        private long pageNum;

        // size in bytes of free space in data page
        private short freeSpace;

        // creates an invalid data page entry (one where no data page has been allocated yet).
        private DataPageEntry() {
            this(DiskSpaceManager.INVALID_PAGE_NUM, (short) -1);
        }

        private DataPageEntry(long pageNum, short freeSpace) {
            this.pageNum = pageNum;
            this.freeSpace = freeSpace;
        }

        // returns if data page entry refers to a valid data page
        private boolean isValid() {
            return this.pageNum != DiskSpaceManager.INVALID_PAGE_NUM;
        }

        private void toBytes(Buffer b) {
            b.putLong(pageNum).putShort(freeSpace);
        }

        private static DataPageEntry fromBytes(Buffer b) {
            return new DataPageEntry(b.getLong(), b.getShort());
        }

        @Override
        public String toString() {
            return "[Page " + pageNum + ", " + freeSpace + " free]";
        }
    }

    /**
     * Represents a single header page.
     */
    private class HeaderPage implements BacktrackingIterable<Page> {
        private HeaderPage nextPage;
        private Page page;
        private short numDataPages;
        private int headerOffset;

        private HeaderPage(long pageNum, int headerOffset, boolean firstHeader) {
            this.page = bufferManager.fetchPage(lockContext, pageNum);
            // We do not lock header pages for the entirety of the transaction. Instead, we simply
            // use the buffer frame lock (from pinning) to ensure that one transaction writes at a time.
            // This does mean that we do not have complete isolation in the header pages, but this does not
            // really matter, as the only observable effect is that a transaction may be told to use a different
            // data page, which is perfectly fine.
            this.page.disableLocking();
            this.numDataPages = 0;
            long nextPageNum;
            try {
                Buffer pageBuffer = this.page.getBuffer();
                if (pageBuffer.get() != (byte) 1) {
                    byte[] buf = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
                    Buffer b = ByteBuffer.wrap(buf);
                    // invalid page, initialize empty header page
                    if (firstHeader) {
                        pageDirectoryId = new Random().nextInt();
                    }
                    b.position(0).put((byte) 1).putInt(pageDirectoryId).putLong(DiskSpaceManager.INVALID_PAGE_NUM);
                    DataPageEntry invalidPageEntry = new DataPageEntry();
                    for (int i = 0; i < HEADER_ENTRY_COUNT; ++i) {
                        invalidPageEntry.toBytes(b);
                    }
                    nextPageNum = -1L;

                    pageBuffer.put(buf, 0, buf.length);
                } else {
                    // load header page
                    if (firstHeader) {
                        pageDirectoryId = pageBuffer.getInt();
                    } else if (pageDirectoryId != pageBuffer.getInt()) {
                        throw new PageException("header page page directory id does not match");
                    }
                    nextPageNum = pageBuffer.getLong();
                    for (int i = 0; i < HEADER_ENTRY_COUNT; ++i) {
                        DataPageEntry dpe = DataPageEntry.fromBytes(pageBuffer);
                        if (dpe.isValid()) {
                            ++this.numDataPages;
                        }
                    }
                }
            } finally {
                this.page.unpin();
            }
            this.headerOffset = headerOffset;
            if (nextPageNum == DiskSpaceManager.INVALID_PAGE_NUM) {
                this.nextPage = null;
            } else {
                this.nextPage = new HeaderPage(nextPageNum, headerOffset + 1, false);
            }
        }

        // add a new header page
        private void addNewHeaderPage() {
            if (this.nextPage != null) {
                this.nextPage.addNewHeaderPage();
                return;
            }
            Page page = bufferManager.fetchNewPage(lockContext, partNum);
            this.page.pin();
            try {
                this.nextPage = new HeaderPage(page.getPageNum(), headerOffset + 1, false);
                this.page.getBuffer().position(1).putLong(page.getPageNum());
            } finally {
                this.page.unpin();
                page.unpin();
            }
        }

        // gets and loads a page with the required free space
        private Page loadPageWithSpace(short requiredSpace) {
            this.page.pin();
            try {
                Buffer b = this.page.getBuffer();
                b.position(HEADER_HEADER_SIZE);

                // if we have any data page managed by this header page with enough space, return it
                short unusedSlot = -1;
                for (short i = 0; i < HEADER_ENTRY_COUNT; ++i) {
                    DataPageEntry dpe = DataPageEntry.fromBytes(b);
                    if (!dpe.isValid()) {
                        if (unusedSlot == -1) {
                            unusedSlot = i;
                        }
                        continue;
                    }
                    if (dpe.freeSpace >= requiredSpace) {
                        dpe.freeSpace -= requiredSpace;
                        b.position(b.position() - DataPageEntry.SIZE);
                        dpe.toBytes(b);

                        return bufferManager.fetchPage(lockContext, dpe.pageNum);
                    }
                }

                // if we have any unused slot in this header page, allocate a new data page
                if (unusedSlot != -1) {
                    Page page = bufferManager.fetchNewPage(lockContext, partNum);
                    DataPageEntry dpe = new DataPageEntry(page.getPageNum(),
                                                          (short) (EFFECTIVE_PAGE_SIZE - emptyPageMetadataSize - requiredSpace));

                    b.position(HEADER_HEADER_SIZE + DataPageEntry.SIZE * unusedSlot);
                    dpe.toBytes(b);

                    page.getBuffer().putInt(pageDirectoryId).putInt(headerOffset).putShort(unusedSlot);

                    ++this.numDataPages;
                    return page;
                }

                // if we have no next header page, make one
                if (this.nextPage == null) {
                    this.addNewHeaderPage();
                }

                // no space on this header page, try next one
                return this.nextPage.loadPageWithSpace(requiredSpace);
            } finally {
                this.page.unpin();
            }
        }

        // updates free space
        private void updateSpace(Page dataPage, short index, short newFreeSpace) {
            this.page.pin();
            try {
                if (newFreeSpace < EFFECTIVE_PAGE_SIZE - emptyPageMetadataSize) {
                    // write new free space to disk
                    Buffer b = this.page.getBuffer();
                    b.position(HEADER_HEADER_SIZE + DataPageEntry.SIZE * index);
                    DataPageEntry dpe = DataPageEntry.fromBytes(b);
                    dpe.freeSpace = newFreeSpace;
                    b.position(HEADER_HEADER_SIZE + DataPageEntry.SIZE * index);
                    dpe.toBytes(b);
                } else {
                    // the entire page is free; free it
                    Buffer b = this.page.getBuffer();
                    b.position(HEADER_HEADER_SIZE + DataPageEntry.SIZE * index);
                    (new DataPageEntry()).toBytes(b);
                    bufferManager.freePage(dataPage);
                }
            } finally {
                this.page.unpin();
            }
        }

        @Override
        public BacktrackingIterator<Page> iterator() {
            return new HeaderPageIterator();
        }

        // iterator over the data pages managed by this header page
        private class HeaderPageIterator extends IndexBacktrackingIterator<Page> {
            private HeaderPageIterator() {
                super(HEADER_ENTRY_COUNT);
            }

            @Override
            protected int getNextNonEmpty(int currentIndex) {
                HeaderPage.this.page.pin();
                try {
                    Buffer b = HeaderPage.this.page.getBuffer();
                    b.position(HEADER_HEADER_SIZE + DataPageEntry.SIZE * ++currentIndex);
                    for (int i = currentIndex; i < HEADER_ENTRY_COUNT; ++i) {
                        DataPageEntry dpe = DataPageEntry.fromBytes(b);
                        if (dpe.isValid()) {
                            return i;
                        }
                    }
                    return HEADER_ENTRY_COUNT;
                } finally {
                    HeaderPage.this.page.unpin();
                }
            }

            @Override
            protected Page getValue(int index) {
                HeaderPage.this.page.pin();
                try {
                    Buffer b = HeaderPage.this.page.getBuffer();
                    b.position(HEADER_HEADER_SIZE + DataPageEntry.SIZE * index);
                    DataPageEntry dpe = DataPageEntry.fromBytes(b);
                    return new DataPage(pageDirectoryId, bufferManager.fetchPage(lockContext, dpe.pageNum));
                } finally {
                    HeaderPage.this.page.unpin();
                }
            }
        }
    }

    /**
     * Iterator over header pages.
     */
    private class HeaderPageIterator implements BacktrackingIterator<BacktrackingIterable<Page>> {
        private HeaderPage nextPage;
        private HeaderPage prevPage;
        private HeaderPage markedPage;

        private HeaderPageIterator() {
            this.nextPage = firstHeader;
            this.prevPage = null;
            this.markedPage = null;
        }

        @Override
        public boolean hasNext() {
            return this.nextPage != null;
        }

        @Override
        public HeaderPage next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            HeaderPage next = this.nextPage;
            this.prevPage = next;
            this.nextPage = next.nextPage;
            return next;
        }

        @Override
        public void markPrev() {
            if (this.prevPage != null) {
                this.markedPage = this.prevPage;
            }
        }

        @Override
        public void markNext() {
            this.markedPage = this.nextPage;
        }

        @Override
        public void reset() {
            if (this.markedPage != null) {
                this.prevPage = null;
                this.nextPage = this.markedPage;
            }
        }
    }
}
