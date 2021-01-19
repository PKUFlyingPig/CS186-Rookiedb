package edu.berkeley.cs186.database.io;

public interface DiskSpaceManager extends AutoCloseable {
    short PAGE_SIZE = 4096; // size of a page in bytes
    long INVALID_PAGE_NUM = -1L; // a page number that is always invalid

    @Override
    void close();

    /**
     * Allocates a new partition.
     *
     * @return partition number of new partition
     */
    int allocPart();

    /**
     * Allocates a new partition with a specific partition number.
     *
     * @param partNum partition number of new partition
     * @return partition number of new partition
     */
    int allocPart(int partNum);

    /**
     * Releases a partition from use.

     * @param partNum partition number to be released
     */
    void freePart(int partNum);

    /**
     * Allocates a new page.
     * @param partNum partition to allocate new page under
     * @return virtual page number of new page
     */
    long allocPage(int partNum);

    /**
     * Allocates a new page with a specific page number.
     * @param pageNum page number of new page
     * @return virtual page number of new page
     */
    long allocPage(long pageNum);

    /**
     * Frees a page. The page cannot be used after this call.
     * @param page virtual page number of page to be released
     */
    void freePage(long page);

    /**
     * Reads a page.
     *
     * @param page number of page to be read
     * @param buf byte buffer whose contents will be filled with page data
     */
    void readPage(long page, byte[] buf);

    /**
     * Writes to a page.
     *
     * @param page number of page to be read
     * @param buf byte buffer that contains the new page data
     */
    void writePage(long page, byte[] buf);

    /**
     * Checks if a page is allocated
     *
     * @param page number of page to check
     * @return true if the page is allocated, false otherwise
     */
    boolean pageAllocated(long page);

    /**
     * Gets partition number from virtual page number
     * @param page virtual page number
     * @return partition number
     */
    static int getPartNum(long page) {
        return (int) (page / 10000000000L);
    }

    /**
     * Gets data page number from virtual page number
     * @param page virtual page number
     * @return data page number
     */
    static int getPageNum(long page) {
        return (int) (page % 10000000000L);
    }

    /**
     * Gets the virtual page number given partition/data page number
     * @param partNum partition number
     * @param pageNum data page number
     * @return virtual page number
     */
    static long getVirtualPageNum(int partNum, int pageNum) {
        return partNum * 10000000000L + pageNum;
    }

}
