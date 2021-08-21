package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.common.AbstractBuffer;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.PageException;

/**
 * Represents a page loaded in memory (as opposed to the buffer frame it's in). Wraps
 * around buffer manager frames, and requests the page be loaded into memory as necessary.
 */
public class Page {
    // lock context for this page
    private LockContext lockContext;

    // buffer manager frame for this page's data (potentially invalidated)
    private BufferFrame frame;

    /**
     * Create a page handle with the given buffer frame
     *
     * @param lockContext the lock context
     * @param frame the buffer manager frame for this page
     */
    Page(LockContext lockContext, BufferFrame frame) {
        this.lockContext = lockContext;
        this.frame = frame;
    }

    /**
     * Creates a page handle given another page handle
     *
     * @param page page handle to copy
     */
    protected Page(Page page) {
        this.lockContext = page.lockContext;
        this.frame = page.frame;
    }

    /**
     * Disables locking on this page handle.
     */
    public void disableLocking() {
        this.lockContext = new DummyLockContext("_dummyPage");
    }

    /**
     * Gets a Buffer object for more convenient access to the page.
     *
     * @return Buffer object over this page
     */
    public Buffer getBuffer() {
        return new PageBuffer();
    }

    /**
     * Reads num bytes from offset position into buf.
     *
     * @param position the offset in the page to read from
     * @param num the number of bytes to read
     * @param buf the buffer to put the bytes into
     */
    private void readBytes(int position, int num, byte[] buf) {
        if (position < 0 || num < 0) {
            throw new PageException("position or num can't be negative");
        }
        if (frame.getEffectivePageSize() < position + num) {
            throw new PageException("readBytes is out of bounds");
        }
        if (buf.length < num) {
            throw new PageException("num bytes to read is longer than buffer");
        }

        this.frame.readBytes((short) position, (short) num, buf);
    }

    /**
     * Read all the bytes in file.
     *
     * @return a new byte array with all the bytes in the file
     */
    private byte[] readBytes() {
        byte[] data = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        getBuffer().get(data);
        return data;
    }

    /**
     * Write num bytes from buf at offset position.
     *
     * @param position the offest in the file to write to
     * @param num the number of bytes to write
     * @param buf the source for the write
     */
    private void writeBytes(int position, int num, byte[] buf) {
        if (buf.length < num) {
            throw new PageException("num bytes to write is longer than buffer");
        }

        if (position < 0 || num < 0) {
            throw new PageException("position or num can't be negative");
        }

        if (frame.getEffectivePageSize() < num + position) {
            throw new PageException("writeBytes would go out of bounds");
        }

        this.frame.writeBytes((short) position, (short) num, buf);
    }

    /**
     * Write all the bytes in file.
     */
    private void writeBytes(byte[] data) {
        getBuffer().put(data);
    }

    /**
     * Completely wipe (zero out) the page.
     */
    public void wipe() {
        byte[] zeros = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        writeBytes(zeros);
    }

    /**
     * Force the page to disk.
     */
    public void flush() {
        this.frame.flush();
    }

    /**
     * Loads the page into a frame (if necessary) and pins it.
     */
    public void pin() {
        this.frame = this.frame.requestValidFrame();
    }

    /**
     * Unpins the frame containing this page. Does not flush immediately.
     */
    public void unpin() {
        this.frame.unpin();
    }

    /**
     * @return the virtual page number of this page
     */
    public long getPageNum() {
        return this.frame.getPageNum();
    }

    /**
     * @param pageLSN the new pageLSN of this page - should only be used by recovery
     */
    public void setPageLSN(long pageLSN) {
        this.frame.setPageLSN(pageLSN);
    }

    /**
     * @return the pageLSN of this page
     */
    public long getPageLSN() {
        return this.frame.getPageLSN();
    }

    @Override
    public String toString() {
        return "Page " + this.frame.getPageNum();
    }

    @Override
    public boolean equals(Object b) {
        if (!(b instanceof Page)) {
            return false;
        }
        return ((Page) b).getPageNum() == getPageNum();
    }

    /**
     * Implementation of Buffer for the page data. All reads/writes ultimately wrap around
     * Page#readBytes and Page#writeBytes, which delegates work to the buffer manager.
     */
    private class PageBuffer extends AbstractBuffer {
        private int offset;

        private PageBuffer() {
            this(0, 0);
        }

        private PageBuffer(int offset, int position) {
            super(position);
            this.offset = offset;
        }

        /**
         * All read operations through the Page object must run through this method.
         *
         * @param dst destination byte buffer
         * @param offset offset into page to start reading
         * @param length number of bytes to read
         * @return this
         */
        @Override
        public Buffer get(byte[] dst, int offset, int length) {
            // TODO(proj4_part2): Update the following line
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);
            Page.this.readBytes(this.offset + offset, length, dst);
            return this;
        }

        /**
         * All write operations through the Page object must run through this method.
         *
         * @param src source byte buffer (to copy to the page)
         * @param offset offset into page to start writing
         * @param length number of bytes to write
         * @return this
         */
        @Override
        public Buffer put(byte[] src, int offset, int length) {
            // TODO(proj4_part2): Update the following line
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);
            Page.this.writeBytes(this.offset + offset, length, src);
            return this;
        }

        /**
         * Create a new PageBuffer starting at the current offset.
         * @return new PageBuffer starting at the current offset
         */
        @Override
        public Buffer slice() {
            return new PageBuffer(offset + position(), 0);
        }

        /**
         * Create a duplicate PageBuffer object
         * @return PageBuffer that is functionally identical to this one
         */
        @Override
        public Buffer duplicate() {
            return new PageBuffer(offset, position());
        }
    }
}
