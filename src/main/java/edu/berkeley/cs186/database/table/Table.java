package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.Bits;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterable;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.ConcatBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.IndexBacktrackingIterator;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * # Overview
 * A Table represents a database table with which users can insert, get,
 * update, and delete records:
 *
 *   // Create a brand new table t(x: int, y: int) which is persisted in the
 *   // the heap file associated with `pageDirectory`.
 *   List<String> fieldNames = Arrays.asList("x", "y");
 *   List<String> fieldTypes = Arrays.asList(Type.intType(), Type.intType());
 *   Schema schema = new Schema(fieldNames, fieldTypes);
 *   Table t = new Table("t", schema, pageDirectory, new DummyLockContext());
 *
 *   // Insert, get, update, and delete records.
 *   List<DataBox> a = Arrays.asList(new IntDataBox(1), new IntDataBox(2));
 *   List<DataBox> b = Arrays.asList(new IntDataBox(3), new IntDataBox(4));
 *   RecordId rid = t.addRecord(a);
 *   Record ra = t.getRecord(rid);
 *   t.updateRecord(b, rid);
 *   Record rb = t.getRecord(rid);
 *   t.deleteRecord(rid);
 *
 * # Persistence
 * Every table is persisted in its own PageDirectory object (passed into the constructor),
 * which interfaces with the BufferManager and DiskSpaceManager to save it to disk.
 *
 * A table can be loaded again by simply constructing it with the same parameters.
 *
 * # Storage Format
 * Now, we discuss how tables serialize their data.
 *
 * All pages are data pages - there are no header pages, because all metadata is
 * stored elsewhere (as rows in the _metadata.tables table). Every data
 * page begins with a n-byte bitmap followed by m records. The bitmap indicates
 * which records in the page are valid. The values of n and m are set to maximize the
 * number of records per page (see computeDataPageNumbers for details).
 *
 * For example, here is a cartoon of what a table's file would look like if we
 * had 5-byte pages and 1-byte records:
 *
 *          +----------+----------+----------+----------+----------+ \
 *   Page 0 | 1001xxxx | 01111010 | xxxxxxxx | xxxxxxxx | 01100001 |  |
 *          +----------+----------+----------+----------+----------+  |
 *   Page 1 | 1101xxxx | 01110010 | 01100100 | xxxxxxxx | 01101111 |  |- data
 *          +----------+----------+----------+----------+----------+  |
 *   Page 2 | 0011xxxx | xxxxxxxx | xxxxxxxx | 01111010 | 00100001 |  |
 *          +----------+----------+----------+----------+----------+ /
 *           \________/ \________/ \________/ \________/ \________/
 *            bitmap     record 0   record 1   record 2   record 3
 *
 *  - The first page (Page 0) is a data page. The first byte of this data page
 *    is a bitmap, and the next four bytes are each records. The first and
 *    fourth bit are set indicating that record 0 and record 3 are valid.
 *    Record 1 and record 2 are invalid, so we ignore their contents.
 *    Similarly, the last four bits of the bitmap are unused, so we ignore
 *    their contents.
 *  - The second and third page (Page 1 and 2) are also data pages and are
 *    formatted similar to Page 0.
 *
 *  When we add a record to a table, we add it to the very first free slot in
 *  the table. See addRecord for more information.
 *
 * Some tables have large records. In order to efficiently handle tables with
 * large records (that still fit on a page), we format these tables a bit differently,
 * by giving each record a full page. Tables with full page records do not have a bitmap.
 * Instead, each allocated page is a single record, and we indicate that a page does
 * not contain a record by simply freeing the page.
 *
 * In some cases, this behavior may be desirable even for small records (our database
 * only supports locking at the page level, so in cases where tuple-level locks are
 * necessary even at the cost of an I/O per tuple, a full page record may be desirable),
 * and may be explicitly toggled on with the setFullPageRecords method.
 */
public class Table implements BacktrackingIterable<Record> {
    // The name of the table.
    private String name;

    // The schema of the table.
    private Schema schema;

    // The page directory persisting the table.
    private PageDirectory pageDirectory;

    // The size (in bytes) of the bitmap found at the beginning of each data page.
    private int bitmapSizeInBytes;

    // The number of records on each data page.
    private int numRecordsPerPage;

    // The lock context of the table.
    private LockContext tableContext;

    // Statistics about the contents of the database.
    Map<String, TableStats> stats;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Load a table named `name` with schema `schema` from `pageDirectory`. `lockContext`
     * is the lock context of the table (use a DummyLockContext() to disable locking). A
     * new table will be created if none exists in the pageDirectory.
     */
    public Table(String name, Schema schema, PageDirectory pageDirectory, LockContext lockContext, Map<String, TableStats> stats) {
        this.name = name;
        this.pageDirectory = pageDirectory;
        this.schema = schema;
        this.tableContext = lockContext;

        this.bitmapSizeInBytes = computeBitmapSizeInBytes(pageDirectory.getEffectivePageSize(), schema);
        this.numRecordsPerPage = computeNumRecordsPerPage(pageDirectory.getEffectivePageSize(), schema);
        // mark everything that is not used for records as metadata
        this.pageDirectory.setEmptyPageMetadataSize((short) (pageDirectory.getEffectivePageSize() - numRecordsPerPage
                                               * schema.getSizeInBytes()));
        this.stats = stats;
        if (!this.stats.containsKey(name)) this.stats.put(name, new TableStats(this.schema, this.numRecordsPerPage));
    }

    public Table(String name, Schema schema, PageDirectory pageDirectory, LockContext lockContext) {
        this(name, schema, pageDirectory, lockContext, new HashMap<>());
    }
    // Accessors ///////////////////////////////////////////////////////////////
    public String getName() {
        return name;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getNumRecordsPerPage() {
        return numRecordsPerPage;
    }

    public void setFullPageRecords() {
        numRecordsPerPage = 1;
        bitmapSizeInBytes = 0;
        pageDirectory.setEmptyPageMetadataSize((short) (pageDirectory.getEffectivePageSize() -
                                          schema.getSizeInBytes()));
    }

    public TableStats getStats() {
        return this.stats.get(name);
    }

    public int getNumDataPages() {
        return this.pageDirectory.getNumDataPages();
    }

    public int getPartNum() {
        return pageDirectory.getPartNum();
    }

    private byte[] getBitMap(Page page) {
        if (bitmapSizeInBytes > 0) {
            byte[] bytes = new byte[bitmapSizeInBytes];
            page.getBuffer().get(bytes, 0, bitmapSizeInBytes);
            return bytes;
        } else {
            return new byte[] {(byte) 0xFF};
        }
    }

    private void writeBitMap(Page page, byte[] bitmap) {
        if (bitmapSizeInBytes > 0) {
            assert bitmap.length == bitmapSizeInBytes;
            page.getBuffer().put(bitmap, 0, bitmapSizeInBytes);
        }
    }

    private static int computeBitmapSizeInBytes(int pageSize, Schema schema) {
        int recordsPerPage = computeNumRecordsPerPage(pageSize, schema);
        if (recordsPerPage == 1) return 0;
        if (recordsPerPage % 8 == 0) return recordsPerPage / 8;
        return recordsPerPage / 8 + 1;
    }

    /**
     * Computes the maximum number of records per page of the given `schema` that
     * can fit on a page with `pageSize` bytes of space, including the overhead
     * for the bitmap. In most cases this can be computed as the number of bits
     * in the page floor divided by the number of bits per record. In the
     * special case where only a single record can fit in a page, no bitmap is
     * needed.
     * @param pageSize size of page in bytes
     * @param schema schema for the records to be stored on this page
     * @return the maximum number of records that can be stored per page
     */
    public static int computeNumRecordsPerPage(int pageSize, Schema schema) {
        int schemaSize = schema.getSizeInBytes();
        if (schemaSize > pageSize) {
            throw new DatabaseException(String.format(
                    "Schema of size %f bytes is larger than effective page size",
                    schemaSize
            ));
        }
        if (2 * schemaSize + 1 > pageSize) {
            // special case: full page records with no bitmap. Checks if two
            // records + bitmap is larger than the effective page size
            return 1;
        }
        // +1 for space in bitmap
        int recordOverheadInBits = 1 + 8 * schema.getSizeInBytes();
        int pageSizeInBits = pageSize  * 8;
        return pageSizeInBits / recordOverheadInBits;
    }

    // Modifiers ///////////////////////////////////////////////////////////////
    /**
     * buildStatistics builds histograms on each of the columns of a table. Running
     * it multiple times refreshes the statistics
     */
    public void buildStatistics(int buckets) {
        this.stats.get(name).refreshHistograms(buckets, this);
    }

    private synchronized void insertRecord(Page page, int entryNum, Record record) {
        int offset = bitmapSizeInBytes + (entryNum * schema.getSizeInBytes());
        page.getBuffer().position(offset).put(record.toBytes(schema));
    }

    /**
     * addRecord adds a record to this table and returns the record id of the
     * newly added record. stats, freePageNums, and numRecords are updated
     * accordingly. The record is added to the first free slot of the first free
     * page (if one exists, otherwise one is allocated). For example, if the
     * first free page has bitmap 0b11101000, then the record is inserted into
     * the page with index 3 and the bitmap is updated to 0b11111000.
     */
    public synchronized RecordId addRecord(Record record) {
        record = schema.verify(record);
        Page page = pageDirectory.getPageWithSpace(schema.getSizeInBytes());
        try {
            // Find the first empty slot in the bitmap.
            // entry number of the first free slot and store it in entryNum; and (2) we
            // count the total number of entries on this page.
            byte[] bitmap = getBitMap(page);
            int entryNum = 0;
            for (; entryNum < numRecordsPerPage; ++entryNum) {
                if (Bits.getBit(bitmap, entryNum) == Bits.Bit.ZERO) {
                    break;
                }
            }
            if (numRecordsPerPage == 1) {
                entryNum = 0;
            }
            assert (entryNum < numRecordsPerPage);

            // Insert the record and update the bitmap.
            insertRecord(page, entryNum, record);
            Bits.setBit(bitmap, entryNum, Bits.Bit.ONE);
            writeBitMap(page, bitmap);

            // Update the metadata.
            stats.get(name).addRecord(record);
            return new RecordId(page.getPageNum(), (short) entryNum);
        } finally {
            page.unpin();
        }
    }

    /**
     * Retrieves a record from the table, throwing an exception if no such record
     * exists.
     */
    public synchronized Record getRecord(RecordId rid) {
        validateRecordId(rid);
        Page page = fetchPage(rid.getPageNum());
        try {
            byte[] bitmap = getBitMap(page);
            if (Bits.getBit(bitmap, rid.getEntryNum()) == Bits.Bit.ZERO) {
                String msg = String.format("Record %s does not exist.", rid);
                throw new DatabaseException(msg);
            }

            int offset = bitmapSizeInBytes + (rid.getEntryNum() * schema.getSizeInBytes());
            Buffer buf = page.getBuffer();
            buf.position(offset);
            return Record.fromBytes(buf, schema);
        } finally {
            page.unpin();
        }
    }

    /**
     * Overwrites an existing record with new values and returns the existing
     * record. stats is updated accordingly. An exception is thrown if rid does
     * not correspond to an existing record in the table.
     */
    public synchronized Record updateRecord(RecordId rid, Record updated) {
        validateRecordId(rid);
        // If we're updating a record we'll need exclusive access to the page
        // its on.
        LockContext pageContext = tableContext.childContext(rid.getPageNum());
        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(pageContext, LockType.NL);

        Record newRecord = schema.verify(updated);
        Record oldRecord = getRecord(rid);

        Page page = fetchPage(rid.getPageNum());
        try {
            insertRecord(page, rid.getEntryNum(), newRecord);

            this.stats.get(name).removeRecord(oldRecord);
            this.stats.get(name).addRecord(newRecord);
            return oldRecord;
        } finally {
            page.unpin();
        }
    }

    /**
     * Deletes and returns the record specified by rid from the table and updates
     * stats, freePageNums, and numRecords as necessary. An exception is thrown
     * if rid does not correspond to an existing record in the table.
     */
    public synchronized Record deleteRecord(RecordId rid) {
        validateRecordId(rid);
        LockContext pageContext = tableContext.childContext(rid.getPageNum());

        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(pageContext, LockType.NL);

        Page page = fetchPage(rid.getPageNum());
        try {
            Record record = getRecord(rid);

            byte[] bitmap = getBitMap(page);
            Bits.setBit(bitmap, rid.getEntryNum(), Bits.Bit.ZERO);
            writeBitMap(page, bitmap);

            stats.get(name).removeRecord(record);
            int numRecords = numRecordsPerPage == 1 ? 0 : numRecordsOnPage(page);
            pageDirectory.updateFreeSpace(page,
                                     (short) ((numRecordsPerPage - numRecords) * schema.getSizeInBytes()));
            return record;
        } finally {
            page.unpin();
        }
    }

    @Override
    public String toString() {
        return "Table " + name;
    }

    // Helpers /////////////////////////////////////////////////////////////////
    private Page fetchPage(long pageNum) {
        try {
            return pageDirectory.getPage(pageNum);
        } catch (PageException e) {
            throw new DatabaseException(e);
        }
    }

    private int numRecordsOnPage(Page page) {
        byte[] bitmap = getBitMap(page);
        int numRecords = 0;
        for (int i = 0; i < numRecordsPerPage; ++i) {
            if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                numRecords++;
            }
        }
        return numRecords;
    }

    private void validateRecordId(RecordId rid) {
        int e = rid.getEntryNum();

        if (e < 0) {
            String msg = String.format("Invalid negative entry number %d.", e);
            throw new DatabaseException(msg);
        }

        if (e >= numRecordsPerPage) {
            String msg = String.format(
                             "There are only %d records per page, but record %d was requested.",
                             numRecordsPerPage, e);
            throw new DatabaseException(msg);
        }
    }

    // Iterators ///////////////////////////////////////////////////////////////

    /**
     * @return Performs a full scan on the table to return id's of all existing
     * records
     */
    public BacktrackingIterator<RecordId> ridIterator() {
        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.NL);

        BacktrackingIterator<Page> iter = pageDirectory.iterator();
        return new ConcatBacktrackingIterator<>(new PageIterator(iter, false));
    }

    /**
     * @param rids an iterator of record IDs for records in this table
     * @return an iterator over the records corresponding to the record IDs. If
     * the record ID iterator supported backtracking, the new record iterator
     * will also support backtracking.
     */
    public BacktrackingIterator<Record> recordIterator(Iterator<RecordId> rids) {
        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.NL);
        return new RecordIterator(rids);
    }

    public BacktrackingIterator<Page> pageIterator() {
        return pageDirectory.iterator();
    }

    @Override
    public BacktrackingIterator<Record> iterator() {
        // returns an iterator over all the records in this table
        return new RecordIterator(ridIterator());
    }

    /**
     * RIDPageIterator is a BacktrackingIterator over the RecordIds of a single
     * page of the table.
     *
     * See comments on the BacktrackingIterator interface for how mark and reset
     * should function.
     */
    class RIDPageIterator extends IndexBacktrackingIterator<RecordId> {
        private Page page;
        private byte[] bitmap;

        RIDPageIterator(Page page) {
            super(numRecordsPerPage);
            this.page = page;
            this.bitmap = getBitMap(page);
            page.unpin();
        }

        @Override
        protected int getNextNonEmpty(int currentIndex) {
            for (int i = currentIndex + 1; i < numRecordsPerPage; ++i) {
                if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                    return i;
                }
            }
            return numRecordsPerPage;
        }

        @Override
        protected RecordId getValue(int index) {
            return new RecordId(page.getPageNum(), (short) index);
        }
    }

    private class PageIterator implements BacktrackingIterator<BacktrackingIterable<RecordId>> {
        private BacktrackingIterator<Page> sourceIterator;
        private boolean pinOnFetch;

        private PageIterator(BacktrackingIterator<Page> sourceIterator, boolean pinOnFetch) {
            this.sourceIterator = sourceIterator;
            this.pinOnFetch = pinOnFetch;
        }

        @Override
        public void markPrev() {
            sourceIterator.markPrev();
        }

        @Override
        public void markNext() {
            sourceIterator.markNext();
        }

        @Override
        public void reset() {
            sourceIterator.reset();
        }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public BacktrackingIterable<RecordId> next() {
            return new InnerIterable(sourceIterator.next());
        }

        private class InnerIterable implements BacktrackingIterable<RecordId> {
            private Page baseObject;

            private InnerIterable(Page baseObject) {
                this.baseObject = baseObject;
                if (!pinOnFetch) {
                    baseObject.unpin();
                }
            }

            @Override
            public BacktrackingIterator<RecordId> iterator() {
                baseObject.pin();
                return new RIDPageIterator(baseObject);
            }
        }
    }

    /**
     * Wraps an iterator of record ids to form an iterator over records.
     */
    private class RecordIterator implements BacktrackingIterator<Record> {
        private Iterator<RecordId> ridIter;

        public RecordIterator(Iterator<RecordId> ridIter) {
            this.ridIter = ridIter;
        }

        @Override
        public boolean hasNext() {
            return ridIter.hasNext();
        }

        @Override
        public Record next() {
            try {
                return getRecord(ridIter.next());
            } catch (DatabaseException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void markPrev() {
            if (ridIter instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIter).markPrev();
            } else {
                throw new UnsupportedOperationException("Cannot markPrev using underlying iterator");
            }
        }

        @Override
        public void markNext() {
            if (ridIter instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIter).markNext();
            } else {
                throw new UnsupportedOperationException("Cannot markNext using underlying iterator");
            }
        }

        @Override
        public void reset() {
            if (ridIter instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIter).reset();
            } else {
                throw new UnsupportedOperationException("Cannot reset using underlying iterator");
            }
        }
    }
}

