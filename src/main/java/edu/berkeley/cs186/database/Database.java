package edu.berkeley.cs186.database;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.UnaryOperator;

import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.*;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.index.BPlusTree;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.io.*;
import edu.berkeley.cs186.database.memory.*;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.SequentialScanOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.recovery.*;
import edu.berkeley.cs186.database.table.*;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

/**
 * Database objects keeps track of transactions, tables, and indices
 * and delegates work to its disk manager, buffer manager, lock manager and
 * recovery manager.
 *
 * A Database instance operates on files in the directory specified by the `fileDir`
 * argument in the constructors. The files in this directory will be modified by
 * a DiskSpaceManager instance. Upon starting up for the following partitions
 * are allocated through the disk space manager:
 *  - Partition 0: used for log records from the recovery manager
 *  - Partition 1: used by the _metadata.tables table, which persists
 *    information about user created tables
 *  - Partition 2: used by the _metadata.indices table, which persists
 *    information about user created indices
 *
 * Each partition corresponds to a file in `fileDir`. The remaining partitions
 * are used for user created tables and are allocated as tables are created.
 *
 * Metadata tables are manually synchronized and use a special locking hierarchy
 * to improve concurrency. The methods to lock and access metadata has already
 * been implemented.
 * - _metadata.tables is a child resource of the database. The children of
 *   _metadata.tables are the names of a regular user tables. For example, if a
 *   user wants exclusive access on the metadata of the table `myTable`, they
 *   would have to acquire an X lock on the resource `database/_metadata.tables/mytable`.
 *
 * - _metadata.indices is a child resource of the database. The children of
 *   _metadata.indices are the names of regular user tables. The grandchildren are
 *   the names of indices. For example, if a user wants to get shared access to
 *   the index on column `rowId` of `someTable`, they would need to acquire an
 *   S lock on `database/_metadata.indices/someTable/rowId`. If a user wanted
 *   to acquire exclusive access on all of the indices of `someTable` (for example
 *   to insert a new record into every index) they would need to acquire an
 *   X lock on `database/_metadata.indices/someTable`.
 */
public class Database implements AutoCloseable {
    private static final String METADATA_TABLE_PREFIX = "_metadata.";
    private static final String TABLE_INFO_TABLE_NAME = METADATA_TABLE_PREFIX + "tables";
    private static final String INDEX_INFO_TABLE_NAME = METADATA_TABLE_PREFIX + "indices";
    private static final int DEFAULT_BUFFER_SIZE = 262144; // default of 1G
    // effective page size - table metadata size
    private static final int MAX_SCHEMA_SIZE = 4005;

    // _metadata.tables, manages all tables in the database
    private Table tableMetadata;
    // _metadata.indices, manages all indices in the database
    private Table indexMetadata;
    // number of transactions created
    private long numTransactions;

    // lock manager
    private final LockManager lockManager;
    // disk space manager
    private final DiskSpaceManager diskSpaceManager;
    // buffer manager
    private final BufferManager bufferManager;
    // recovery manager
    private final RecoveryManager recoveryManager;
    // thread pool for background tasks
    private final ExecutorService executor;

    // number of pages of memory to use for joins, etc.
    private int workMem = 1024; // default of 4M
    // number of pages of memory available total
    private int numMemoryPages;
    // active transactions
    private Phaser activeTransactions = new Phaser(0);
    // Statistics about the contents of the database.
    private Map<String, TableStats> stats = new ConcurrentHashMap<>();

    /**
     * Creates a new database with:
     * - Default buffer size
     * - Locking disabled (DummyLockManager)
     * - Clock eviction policy
     * - Recovery manager disabled (DummyRecoverManager)
     *
     * @param fileDir the directory to put the table files in
     */
    public Database(String fileDir) {
        this (fileDir, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new database with defaults:
     * - Locking disabled (DummyLockManager)
     * - Clock eviction policy
     * - Recovery manager disabled (DummyRecoverManager)
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     */
    public Database(String fileDir, int numMemoryPages) {
        this(fileDir, numMemoryPages, new DummyLockManager());
    }

    /**
     * Creates a new database with defaults:
     * - Clock eviction policy
     * - Recovery manager disabled (DummyRecoverManager)
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager) {
        this(fileDir, numMemoryPages, lockManager, new ClockEvictionPolicy());
    }

    /**
     * Creates a new database with recovery disabled (DummyRecoveryManager)
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     * @param policy eviction policy for buffer cache
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager,
                    EvictionPolicy policy) {
        this(fileDir, numMemoryPages, lockManager, policy, false);
    }

    /**
     * Creates a new database.
     *
     * @param fileDir the directory to put the table files in
     * @param numMemoryPages the number of pages of memory in the buffer cache
     * @param lockManager the lock manager
     * @param policy eviction policy for buffer cache
     * @param useRecoveryManager flag to enable or disable the recovery manager (ARIES)
     */
    public Database(String fileDir, int numMemoryPages, LockManager lockManager,
                    EvictionPolicy policy, boolean useRecoveryManager) {
        boolean initialized = setupDirectory(fileDir);

        numTransactions = 0;
        this.numMemoryPages = numMemoryPages;
        this.lockManager = lockManager;
        this.executor = new ThreadPool();

        if (useRecoveryManager) {
            recoveryManager = new ARIESRecoveryManager(lockManager.databaseContext(),
                    this::beginRecoveryTransaction, this::setTransactionCounter, this::getTransactionCounter);
        } else {
            recoveryManager = new DummyRecoveryManager();
        }

        diskSpaceManager = new DiskSpaceManagerImpl(fileDir, recoveryManager);
        bufferManager = new BufferManager(diskSpaceManager, recoveryManager, numMemoryPages,
                                              policy);

        if (!initialized) {
            // create log partition
            diskSpaceManager.allocPart(0);
        }

        recoveryManager.setManagers(diskSpaceManager, bufferManager);

        if (!initialized) {
            recoveryManager.initialize();
        }

        // Analysis and undo are both completed once the next line completes
        Runnable restartRedo = recoveryManager.restart();
        // The redo phase can be run in parallel with the remaining setup
        executor.submit(restartRedo);

        Transaction initTransaction = beginTransaction();
        TransactionContext.setTransaction(initTransaction.getTransactionContext());

        if (!initialized) {
            // _metadata.tables partition, and _metadata.indices partition
            diskSpaceManager.allocPart(1);
            diskSpaceManager.allocPart(2);
        }
        if (!initialized) {
            this.initTableInfo();
            this.initIndexInfo();
        } else {
            this.loadMetadataTables();
        }
        initTransaction.commit();
        TransactionContext.unsetTransaction();
    }

    private boolean setupDirectory(String fileDir) {
        File dir = new File(fileDir);
        boolean initialized = dir.exists();
        if (!initialized) {
            if (!dir.mkdir()) {
                throw new DatabaseException("failed to create directory " + fileDir);
            }
        } else if (!dir.isDirectory()) {
            throw new DatabaseException(fileDir + " is not a directory");
        }
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir.toPath())) {
            initialized = initialized && dirStream.iterator().hasNext();
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return initialized;
    }

    // create _metadata.tables
    private void initTableInfo() {
        long tableInfoPage0 = DiskSpaceManager.getVirtualPageNum(1, 0);
        diskSpaceManager.allocPage(tableInfoPage0);

        LockContext tableInfoContext = new DummyLockContext();
        PageDirectory tableInfoPageDir = new PageDirectory(bufferManager, 1, tableInfoPage0, (short) 0,
                tableInfoContext);
        tableMetadata = new Table(TABLE_INFO_TABLE_NAME, getTableInfoSchema(), tableInfoPageDir,
                              tableInfoContext, stats);
    }

    // create _metadata.indices
    private void initIndexInfo() {
        long indexInfoPage0 = DiskSpaceManager.getVirtualPageNum(2, 0);
        diskSpaceManager.allocPage(indexInfoPage0);
        LockContext indexInfoContext = new DummyLockContext();
        PageDirectory pageDirectory = new PageDirectory(bufferManager, 2, indexInfoPage0, (short) 0,
                                              indexInfoContext);
        indexMetadata = new Table(INDEX_INFO_TABLE_NAME, getIndexInfoSchema(), pageDirectory, indexInfoContext, stats);
    }

    private void loadMetadataTables() {
        // Note: both metadata tables use DummyLockContexts. This is intentional,
        // since we manually synchronize both tables to improve concurrency.

        // load _metadata.tables
        LockContext tableInfoContext = new DummyLockContext();
        PageDirectory tableInfoPageDir = new PageDirectory(bufferManager, 1,
                DiskSpaceManager.getVirtualPageNum(1, 0), (short) 0, tableInfoContext);
        tableMetadata = new Table(TABLE_INFO_TABLE_NAME, getTableInfoSchema(), tableInfoPageDir,
                tableInfoContext, stats);

        // load _metadata.indices
        LockContext indexInfoContext = new DummyLockContext();
        PageDirectory indexInfoPageDir = new PageDirectory(bufferManager, 2,
                DiskSpaceManager.getVirtualPageNum(2, 0), (short) 0, indexInfoContext);
        indexMetadata = new Table(INDEX_INFO_TABLE_NAME, getIndexInfoSchema(), indexInfoPageDir,
                              indexInfoContext, stats);
        indexMetadata.setFullPageRecords();
    }

    // wait for all transactions to finish
    public synchronized void waitAllTransactions() {
        while (!activeTransactions.isTerminated()) {
            activeTransactions.awaitAdvance(activeTransactions.getPhase());
        }
    }

    /**
     * Close this database.
     */
    @Override
    public synchronized void close() {
        if (this.executor.isShutdown()) {
            return;
        }

        // wait for all transactions to terminate
        this.waitAllTransactions();

        // finish executor tasks
        this.executor.shutdown();

        this.bufferManager.evictAll();

        this.recoveryManager.close();

        this.tableMetadata = null;
        this.indexMetadata = null;

        this.bufferManager.close();
        this.diskSpaceManager.close();
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public DiskSpaceManager getDiskSpaceManager() {
        return diskSpaceManager;
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

    public int getWorkMem() {
        // cap work memory at number of memory pages -- this is likely to cause out of memory
        // errors if actually set this high
        return Math.min(this.workMem, this.numMemoryPages);
    }

    public void setWorkMem(int workMem) {
        this.workMem = workMem;
    }

    /**
     * @return Schema for _metadata.tables with fields:
     *   | field name   | field type
     * --+--------------+-------------------------
     * 0 | table_name   | string(32)
     * 1 | part_num     | int
     * 2 | page_num     | long
     * 3 | is_temporary | bool
     * 4 | schema       | byte array(MAX_SCHEMA_SIZE)
     */
    private Schema getTableInfoSchema() {
        return new Schema()
                .add("table_name", Type.stringType(32))
                .add("part_num", Type.intType())
                .add("page_num", Type.longType())
                .add("is_temporary", Type.boolType())
                .add("schema", Type.byteArrayType(MAX_SCHEMA_SIZE));
    }

    /**
     * @return Schema for _metadata.indices with fields:
     *   | field name          | field type
     * --+---------------------+------------
     * 0 | table_name          | string(32)
     * 1 | col_name            | string(32)
     * 2 | order               | int
     * 3 | part_num            | int
     * 4 | root_page_num       | long
     * 5 | key_schema_typeid   | int
     * 6 | key_schema_typesize | int
     * 7 | height              | int
     */
    private Schema getIndexInfoSchema() {
        return new Schema()
                .add("table_name", Type.stringType(32))
                .add("col_name", Type.stringType(32))
                .add("order", Type.intType())
                .add("part_num", Type.intType())
                .add("root_page_num", Type.longType())
                .add("key_schema_typeid", Type.intType())
                .add("key_schema_typesize", Type.intType())
                .add("height", Type.intType());
    }

    // a single row of _metadata.tables
    private static class TableMetadata {
        String tableName;
        int partNum;
        long pageNum;
        boolean isTemporary;
        Schema schema;

        TableMetadata(String tableName) {
            this.tableName = tableName;
            this.partNum = -1;
            this.pageNum = -1;
            this.isTemporary = false;
            this.schema = new Schema();
        }

        TableMetadata(Record record) {
            tableName = record.getValue(0).getString();
            partNum = record.getValue(1).getInt();
            pageNum = record.getValue(2).getLong();
            isTemporary = record.getValue(3).getBool();
            schema = Schema.fromBytes(ByteBuffer.wrap(record.getValue(4).toBytes()));
        }

        Record toRecord() {
            byte[] schemaBytes = schema.toBytes();
            byte[] padded = new byte[MAX_SCHEMA_SIZE];
            System.arraycopy(schemaBytes, 0, padded, 0, schemaBytes.length);
            return new Record(tableName, partNum, pageNum, isTemporary, padded);
        }
    }

    // Trims and lowercases table and column names so that lookups are
    // case-insensitive and format-insensitive
    private String normalize(String name) {
        return name.trim().toLowerCase();
    }

    /**
     * @return (rid, metadata) pairs for all of the tables currently in the
     * database. Assumes that caller has already acquired necessary locks on
     * metadata.
     */
    private List<Pair<RecordId, TableMetadata>> scanTableMetadata() {
        List<Pair<RecordId, TableMetadata>> result = new ArrayList<>();
        synchronized(tableMetadata) {
            for(RecordId rid: (Iterable<RecordId>) tableMetadata::ridIterator) {
                Record record = tableMetadata.getRecord(rid);
                TableMetadata metadata = new TableMetadata(record);
                result.add(new Pair<>(rid, metadata));
            }
        }
        return result;
    }

    /**
     * @param tableName
     * @return the (rid, metadata) pair for the table specified by `tableName`'s
     * entry inside of _metadata.tables. Returns null if the table does not exist.
     */
    private Pair<RecordId, TableMetadata> getTableMetadata(String tableName) {
        tableName = normalize(tableName);
        // We'll need shared access to the entry if it exists in order to read it
        LockUtil.ensureSufficientLockHeld(getTableMetadataContext(tableName), LockType.S);
        for (Pair<RecordId, TableMetadata> p : scanTableMetadata()) {
            String currName = normalize(p.getSecond().tableName);
            if (currName.equals(tableName)) return p;
        }
        return null;
    }

    // TableMetadata -> Table object
    private Table tableFromMetadata(TableMetadata metadata) {
        String tableName = normalize(metadata.tableName);
        LockContext tableContext = getTableContext(tableName);
        long page0 = DiskSpaceManager.getVirtualPageNum(metadata.partNum, 0);
        PageDirectory pd = new PageDirectory(bufferManager, metadata.partNum, page0, (short) 0, tableContext);
        return new Table(metadata.tableName, metadata.schema, pd, tableContext, stats);
    }

    /**
     * @return (rid, metadata) pairs for all of the indices currently
     * in the database. Assumes that caller has already acquired necessary locks
     * on metadata.
     */
    private List<Pair<RecordId, BPlusTreeMetadata>> scanIndexMetadata() {
        List<Pair<RecordId, BPlusTreeMetadata>> result = new ArrayList<>();
        synchronized(indexMetadata) {
            for(RecordId rid: (Iterable<RecordId>) indexMetadata::ridIterator) {
                Record record = indexMetadata.getRecord(rid);
                BPlusTreeMetadata metadata = new BPlusTreeMetadata(record);
                result.add(new Pair<>(rid, metadata));
            }
        }
        return result;
    }

    /**
     * @param tableName
     * @param columnName
     * @return the (rid, metadata) pair corresponding to the index on
     * tableName.columnName inside of _metadata.indices. Returns null if no
     * such index exists.
     */
    private Pair<RecordId, BPlusTreeMetadata> getColumnIndexMetadata(String tableName, String columnName) {
        tableName = normalize(tableName);
        columnName = normalize(columnName);
        // We'll need shared access to the entry if it exists in order to read it
        LockUtil.ensureSufficientLockHeld(getColumnIndexMetadataContext(tableName, columnName), LockType.S);
        for (Pair<RecordId, BPlusTreeMetadata> p: scanIndexMetadata()) {
            BPlusTreeMetadata metadata = p.getSecond();
            String currTableName = normalize(metadata.getTableName());
            String currColumnName = normalize(metadata.getColName());
            if (currTableName.equals(tableName) && currColumnName.equals(columnName)) {
                return p;
            }
        }
        return null;
    }

    /**
     * @param tableName
     * @return a list of (rid, metadata) pairs for all of the indices on the
     * table `tableName` inside of _metadata.indices. Returns an empty list
     * if the table does not exist, or if no indices are built on the table.
     */
    private List<Pair<RecordId, BPlusTreeMetadata>> getTableIndicesMetadata(String tableName) {
        List<Pair<RecordId, BPlusTreeMetadata>> result = new ArrayList<>();
        tableName = normalize(tableName);
        // We'll need shared access to the entry if it exists in order to read it
        LockUtil.ensureSufficientLockHeld(getTableIndexMetadataContext(tableName), LockType.S);
        for (Pair<RecordId, BPlusTreeMetadata> p: scanIndexMetadata()) {
            BPlusTreeMetadata metadata = p.getSecond();
            String currTableName = normalize(metadata.getTableName());
            if (currTableName.equals(tableName)) {
                result.add(p);
            }
        }
        return result;
    }

    // btree metadata -> btree object
    private BPlusTree indexFromMetadata(BPlusTreeMetadata metadata) {
        String tableName = normalize(metadata.getTableName());
        String columnName = normalize(metadata.getColName());
        LockContext indexContext = lockManager.databaseContext().childContext(tableName + "." + columnName);
        return new BPlusTree(bufferManager, metadata, indexContext);
    }

    // get the lock context for database/_metadata.tables
    private LockContext getTableInfoContext() {
        return lockManager.databaseContext().childContext(TABLE_INFO_TABLE_NAME);
    }

    // get the lock context for database/_metadata.indices
    private LockContext getIndexInfoContext() {
        return lockManager.databaseContext().childContext(INDEX_INFO_TABLE_NAME);
    }

    // get the lock context for database/tableName
    private LockContext getTableContext(String tableName) {
        return lockManager.databaseContext().childContext(normalize(tableName));
    }

    // get the lock context for _metadata.tables/tableName
    private LockContext getTableMetadataContext(String tableName) {
        tableName = normalize(tableName);
        return getTableInfoContext().childContext(tableName);
    }

    // get the lock context for database/_metadata.indices/tableName
    private LockContext getTableIndexMetadataContext(String tableName) {
        tableName = normalize(tableName);
        return getIndexInfoContext().childContext(tableName);
    }

    // get the lock context for database/_metadata.indices/tableName/columnName
    private LockContext getColumnIndexMetadataContext(String tableName, String columnName) {
        columnName = normalize(columnName);
        return getTableIndexMetadataContext(tableName).childContext(columnName);
    }

    /**
     * Start a new transaction.
     *
     * @return the new Transaction
     */
    public synchronized Transaction beginTransaction() {
        TransactionImpl t = new TransactionImpl(this.numTransactions, false);
        activeTransactions.register();
        if (activeTransactions.isTerminated()) {
            activeTransactions = new Phaser(1);
        }

        this.recoveryManager.startTransaction(t);
        ++this.numTransactions;
        return t;
    }

    /**
     * Start a transaction for recovery.
     *
     * @param transactionNum transaction number
     * @return the Transaction
     */
    private synchronized Transaction beginRecoveryTransaction(Long transactionNum) {
        this.numTransactions = Math.max(this.numTransactions, transactionNum + 1);

        TransactionImpl t = new TransactionImpl(transactionNum, true);
        activeTransactions.register();
        if (activeTransactions.isTerminated()) {
            activeTransactions = new Phaser(1);
        }

        return t;
    }

    /**
     * Gets the transaction number counter. This is the number of transactions that
     * have been created so far, and also the number of the next transaction to be created.
     */
    private synchronized long getTransactionCounter() {
        return this.numTransactions;
    }

    /**
     * Updates the transaction number counter.
     * @param newTransactionCounter new transaction number counter
     */
    private synchronized void setTransactionCounter(long newTransactionCounter) {
        this.numTransactions = newTransactionCounter;
    }

    private class TransactionContextImpl extends TransactionContext {
        long transNum;
        Map<String, String> aliases;
        Map<String, Table> tempTables;
        long tempTableCounter;

        private TransactionContextImpl(long tNum) {
            this.transNum = tNum;
            this.aliases = new HashMap<>();
            this.tempTables = new HashMap<>();
            this.tempTableCounter = 0;
        }

        @Override
        public long getTransNum() {
            return transNum;
        }

        @Override
        public int getWorkMemSize() {
            return Database.this.getWorkMem();
        }

        @Override
        public String createTempTable(Schema schema) {
            String tempTableName = "tempTable" + tempTableCounter++;
            String tableName = prefixTempTableName(tempTableName);

            int partNum = diskSpaceManager.allocPart();
            long pageNum = diskSpaceManager.allocPage(partNum);
            // We can use dummy contexts since this table will only be visible from the current transaction
            PageDirectory pageDirectory = new PageDirectory(bufferManager, partNum, pageNum, (short) 0, new DummyLockContext());
            tempTables.put(tempTableName, new Table(tableName, schema, pageDirectory, new DummyLockContext(), stats));
            return tempTableName;
        }

        private void deleteTempTable(String tempTableName) {
            if (!this.tempTables.containsKey(tempTableName)) return;
            Table t = tempTables.remove(tempTableName);
            bufferManager.freePart(t.getPartNum());
        }

        @Override
        public void deleteAllTempTables() {
            Set<String> keys = new HashSet<>(tempTables.keySet());
            for (String tableName : keys) deleteTempTable(tableName);
        }

        @Override
        public void setAliasMap(Map<String, String> aliasMap) {
            this.aliases = new HashMap<>(aliasMap);
        }

        @Override
        public void clearAliasMap() {
            this.aliases.clear();
        }

        @Override
        public boolean indexExists(String tableName, String columnName) {
            if (aliases.containsKey(tableName)) tableName = aliases.get(tableName);
            return getColumnIndexMetadata(tableName, columnName) != null;
        }

        @Override
        public void updateIndexMetadata(BPlusTreeMetadata metadata) {
            Record updated = metadata.toRecord();
            String tableName = normalize(metadata.getTableName());
            String columnName = normalize(metadata.getColName());
            // Exclusive access is needed on the index metadata entry to update it
            LockUtil.ensureSufficientLockHeld(getColumnIndexMetadataContext(tableName, columnName), LockType.X);
            for (Pair<RecordId, BPlusTreeMetadata> p: scanIndexMetadata()) {
                RecordId rid = p.getFirst();
                BPlusTreeMetadata currMetadata = p.getSecond();
                String currColumnName = normalize(currMetadata.getColName());
                String currTableName = normalize(currMetadata.getTableName());
                if (currColumnName.equals(columnName) && currTableName.equals(tableName)) {
                    synchronized (indexMetadata) {
                        indexMetadata.updateRecord(rid, updated);
                        return;
                    }
                }
            }
        }

        @Override
        public Iterator<Record> sortedScan(String tableName, String columnName) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            // Since we'll likely scan multiple pages of records, its better
            // to get an S lock on the whole table up front
            LockUtil.ensureSufficientLockHeld(getTableContext(tableName), LockType.S);
            Pair<RecordId, BPlusTreeMetadata> pair = getColumnIndexMetadata(tableName, columnName);

            if (pair != null) {
                BPlusTree tree = indexFromMetadata(pair.getSecond());
                return tab.recordIterator(tree.scanAll());
            } else {
                try {
                    return new SortOperator(this, new SequentialScanOperator(this, tableName),
                        columnName).iterator();
                } catch (Exception e2) {
                    throw new DatabaseException(e2);
                }
            }
        }

        @Override
        public Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            BPlusTree tree = indexFromMetadata(getColumnIndexMetadata(tableName, columnName).getSecond());
            // Since we'll likely scan multiple pages of records, its better
            // to get an S lock on the whole table up front
            LockUtil.ensureSufficientLockHeld(getTableContext(tableName), LockType.S);
            return tab.recordIterator(tree.scanGreaterEqual(startValue));
        }

        @Override
        public Iterator<Record> lookupKey(String tableName, String columnName, DataBox key) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            BPlusTree tree = indexFromMetadata(getColumnIndexMetadata(tableName, columnName).getSecond());
            return tab.recordIterator(tree.scanEqual(key));
        }

        @Override
        public BacktrackingIterator<Record> getRecordIterator(String tableName) {
            return getTable(tableName).iterator();
        }

        @Override
        public boolean contains(String tableName, String columnName, DataBox key) {
            tableName = aliases.getOrDefault(tableName, tableName);
            BPlusTree tree = indexFromMetadata(getColumnIndexMetadata(tableName, columnName).getSecond());
            return tree.get(key).isPresent();
        }

        @Override
        public RecordId addRecord(String tableName, Record record) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            if (tab == null) {
                throw new DatabaseException("table `" + tableName + "` does not exist!");
            }
            RecordId rid = tab.addRecord(record);
            Schema s = tab.getSchema();
            List<String> colNames = s.getFieldNames();

            for (Pair<RecordId, BPlusTreeMetadata> p: getTableIndicesMetadata(tableName)) {
                BPlusTree tree = indexFromMetadata(p.getSecond());
                String column = tree.getMetadata().getColName();
                DataBox key = record.getValue(colNames.indexOf(column));
                tree.put(key, rid);
            }
            return rid;
        }

        @Override
        public RecordId deleteRecord(String tableName, RecordId rid) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            Schema s = tab.getSchema();
            Record record = tab.deleteRecord(rid);
            List<String> colNames = s.getFieldNames();

            for (Pair<RecordId, BPlusTreeMetadata> p: getTableIndicesMetadata(tableName)) {
                BPlusTree tree = indexFromMetadata(p.getSecond());
                String column = tree.getMetadata().getColName();
                DataBox key = record.getValue(colNames.indexOf(column));
                tree.remove(key);
            }
            return rid;
        }

        @Override
        public Record getRecord(String tableName, RecordId rid) {
            return getTable(tableName).getRecord(rid);
        }

        @Override
        public RecordId updateRecord(String tableName, RecordId rid, Record updated) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            Schema s = tab.getSchema();

            Record old = tab.updateRecord(rid, updated);
            List<String> colNames = s.getFieldNames();

            for (Pair<RecordId, BPlusTreeMetadata> p: getTableIndicesMetadata(tableName)) {
                BPlusTree tree = indexFromMetadata(p.getSecond());
                String column = tree.getMetadata().getColName();
                DataBox oldKey = old.getValue(colNames.indexOf(column));
                DataBox newKey = updated.getValue(colNames.indexOf(column));
                tree.remove(oldKey);
                tree.put(newKey, rid);
            }
            return rid;
        }

        @Override
        public void updateRecordWhere(String tableName, String targetColumnName,
                                      UnaryOperator<DataBox> targetValue,
                                      String predColumnName, PredicateOperator predOperator, DataBox predValue) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            Iterator<RecordId> recordIds = tab.ridIterator();

            Schema s = tab.getSchema();
            int uindex = s.getFieldNames().indexOf(targetColumnName);
            int pindex = s.getFieldNames().indexOf(predColumnName);

            while(recordIds.hasNext()) {
                RecordId curRID = recordIds.next();
                Record cur = getRecord(tableName, curRID);
                List<DataBox> recordCopy = cur.getValues();

                if (predOperator == null || predOperator.evaluate(recordCopy.get(pindex), predValue)) {
                    recordCopy.set(uindex, targetValue.apply(recordCopy.get(uindex)));
                    updateRecord(tableName, curRID, new Record(recordCopy));
                }
            }
        }

        @Override
        public void deleteRecordWhere(String tableName, String predColumnName,
                                      PredicateOperator predOperator, DataBox predValue) {
            Table tab = getTable(tableName);
            tableName = tab.getName();
            Iterator<RecordId> recordIds = tab.ridIterator();

            Schema s = tab.getSchema();
            int pindex = s.getFieldNames().indexOf(predColumnName);

            while(recordIds.hasNext()) {
                RecordId curRID = recordIds.next();
                Record cur = getRecord(tableName, curRID);
                List<DataBox> recordCopy = cur.getValues();

                if (predOperator == null || predOperator.evaluate(recordCopy.get(pindex), predValue)) {
                    deleteRecord(tableName, curRID);
                }
            }
        }

        @Override
        public Schema getSchema(String tableName) {
            return getTable(tableName).getSchema();
        }

        @Override
        public Schema getFullyQualifiedSchema(String tableName) {
            Schema schema = getTable(tableName).getSchema();
            Schema qualified = new Schema();
            for (int i = 0; i < schema.size(); i++) {
                qualified.add(
                    tableName + "." + schema.getFieldName(i),
                    schema.getFieldType(i)
                );
            }
            return qualified;
        }

        @Override
        public TableStats getStats(String tableName) {
            return getTable(tableName).getStats();
        }

        @Override
        public int getNumDataPages(String tableName) {
            return getTable(tableName).getNumDataPages();
        }

        @Override
        public int getTreeOrder(String tableName, String columnName) {
            if (aliases.containsKey(tableName)) tableName = aliases.get(tableName);
            Pair<RecordId, BPlusTreeMetadata> pair = getColumnIndexMetadata(tableName, columnName);
            if (pair == null) throw new DatabaseException("Index `" + tableName + "." + columnName + "` does not exist!");
            return pair.getSecond().getOrder();
        }

        @Override
        public int getTreeHeight(String tableName, String columnName) {
            if (aliases.containsKey(tableName)) tableName = aliases.get(tableName);
            Pair<RecordId, BPlusTreeMetadata> pair = getColumnIndexMetadata(tableName, columnName);
            if (pair == null) throw new DatabaseException("Index `" + tableName + "." + columnName + "` does not exist!");
            return pair.getSecond().getHeight();
        }

        @Override
        public void close() {
            try {
                // TODO(proj4_part2)
                return;
            } catch (Exception e) {
                // There's a chance an error message from your release phase
                // logic can get suppressed. This guarantees that the stack
                // trace at least shows up somewhere before suppression.
                // https://stackoverflow.com/questions/7849416/what-is-a-suppressed-exception
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        public String toString() {
            return "Transaction Context for Transaction " + transNum;
        }

        @Override
        public Table getTable(String tableName) {
            if (this.aliases.containsKey(tableName)) {
                tableName = this.aliases.get(tableName);
            }

            if (this.tempTables.containsKey(tableName)) {
                return this.tempTables.get(tableName);
            }
            Pair<RecordId, TableMetadata> pair = Database.this.getTableMetadata(tableName);
            if (pair == null) {
                throw new DatabaseException("Table `" + tableName + "` does not exist!");
            }
            return tableFromMetadata(pair.getSecond());
        }

        private String prefixTempTableName(String name) {
            String prefix = "temp." + transNum + "-";
            if (name.startsWith(prefix)) {
                return name;
            } else {
                return prefix + name;
            }
        }
    }

    private class TransactionImpl extends Transaction {
        private long transNum;
        private boolean recoveryTransaction;
        private TransactionContext transactionContext;

        private TransactionImpl(long transNum, boolean recovery) {
            this.transNum = transNum;
            this.recoveryTransaction = recovery;
            this.transactionContext = new TransactionContextImpl(transNum);
        }

        @Override
        protected void startCommit() {
            // TODO(proj5): replace immediate cleanup() call with job (the commented out code)

            transactionContext.deleteAllTempTables();

            recoveryManager.commit(transNum);

            this.cleanup();
            /*
            executor.execute(this::cleanup);
            */
        }

        @Override
        protected void startRollback() {
            executor.execute(() -> {
                recoveryManager.abort(transNum);
                this.cleanup();
            });
        }

        @Override
        public void cleanup() {
            if (getStatus() == Status.COMPLETE) {
                return;
            }

            if (!this.recoveryTransaction) {
                recoveryManager.end(transNum);
            }

            transactionContext.close();
            activeTransactions.arriveAndDeregister();
        }

        @Override
        public long getTransNum() {
            return transNum;
        }

        @Override
        public void createTable(Schema s, String tableName) {
            if (tableName.contains(".") || tableName.contains(" ") || tableName.length() == 0) {
                throw new IllegalArgumentException("name of new table may not contain '.' or ' ', or be the empty string");
            }
            TransactionContext.setTransaction(transactionContext);
            try {
                // To create the table we'll need exclusive access to it's metadata for the duration of the transaction
                // This way, other transactions won't be able to access it in the event that we abort
                LockUtil.ensureSufficientLockHeld(getTableMetadataContext(tableName), LockType.X);

                // To check whether the table exists we just need to read that table's metadata, if it exists
                Pair<RecordId, TableMetadata> pair = getTableMetadata(tableName);
                if (pair != null) {
                    throw new DatabaseException("table `" + tableName + "` already exists");
                }
                TableMetadata metadata = new TableMetadata(tableName);
                metadata.partNum = diskSpaceManager.allocPart();
                metadata.pageNum = diskSpaceManager.allocPage(metadata.partNum);
                metadata.isTemporary = false;
                metadata.schema = s;
                synchronized (tableMetadata) {
                    tableMetadata.addRecord(metadata.toRecord());
                }
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropTable(String tableName) {
            if (tableName.contains(".") || tableName.contains(" ") || tableName.length() == 0) {
                throw new IllegalArgumentException("name of new table may not contain '.' or ' ', or be the empty string");
            }
            TransactionContext.setTransaction(transactionContext);
            try {
                // To check whether the table exists we just need to read that table's metadata, if it exists
                Pair<RecordId, TableMetadata> pair = getTableMetadata(tableName);
                if (pair == null) {
                    throw new DatabaseException("table `" + tableName + "` already exists");
                }
                // To drop a table we'll need exclusive access to it's metadata and the metadata of its indices
                LockUtil.ensureSufficientLockHeld(getTableMetadataContext(tableName), LockType.X);
                LockUtil.ensureSufficientLockHeld(getTableIndexMetadataContext(tableName), LockType.X);

                for (Pair<RecordId, BPlusTreeMetadata> p: getTableIndicesMetadata(tableName)) {
                    BPlusTreeMetadata tree = p.getSecond();
                    dropIndex(tableName, tree.getColName());
                }
                RecordId rid = getTableMetadata(tableName).getFirst();
                TableMetadata metadata;
                synchronized(tableMetadata) {
                    metadata = new TableMetadata(tableMetadata.deleteRecord(rid));
                }
                bufferManager.freePart(metadata.partNum);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropAllTables() {
            TransactionContext.setTransaction(transactionContext);
            try {
                // For something as drastic as dropping all tables we'll want
                // to get an exclusive lock on the entire database.
                LockUtil.ensureSufficientLockHeld(lockManager.databaseContext(), LockType.X);
                for (Pair<RecordId, TableMetadata> p: scanTableMetadata()) {
                    dropTable(p.getSecond().tableName);
                }
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void createIndex(String tableName, String columnName, boolean bulkLoad) {
            if (tableName.contains(".") || tableName.contains(" ") || tableName.length() == 0) {
                throw new IllegalArgumentException("name of new table may not contain '.' or ' ', or be the empty string");
            }
            TransactionContext.setTransaction(transactionContext);
            try {
                // We want to check that the table exists
                TableMetadata tableMetadata = getTableMetadata(tableName).getSecond();
                if (tableMetadata == null) {
                    throw new DatabaseException("table " + tableName + " does not exist");
                }

                Schema s = tableMetadata.schema;
                List<String> schemaColNames = s.getFieldNames();
                List<Type> schemaColType = s.getFieldTypes();
                if (!schemaColNames.contains(columnName)) {
                    throw new DatabaseException("table " + tableName + " does not have a column " + columnName);
                }

                int columnIndex = schemaColNames.indexOf(columnName);
                Type colType = schemaColType.get(columnIndex);

                // To create the index we'll need an exclusive lock on its metadata
                LockUtil.ensureSufficientLockHeld(getColumnIndexMetadataContext(tableName, columnName), LockType.X);
                Pair<RecordId, BPlusTreeMetadata> pair = getColumnIndexMetadata(tableName, columnName);
                if (pair != null) {
                    throw new DatabaseException("index already exists on " + tableName + "(" + columnName + ")");
                }

                int order = BPlusTree.maxOrder(BufferManager.EFFECTIVE_PAGE_SIZE, colType);
                Record indexEntry = new Record(tableName, columnName, order,
                        diskSpaceManager.allocPart(),
                        diskSpaceManager.INVALID_PAGE_NUM,
                        colType.getTypeId().ordinal(),
                        colType.getSizeInBytes(), -1
                );
                synchronized (indexMetadata) {
                    indexMetadata.addRecord(indexEntry);
                }
                BPlusTreeMetadata metadata = new BPlusTreeMetadata(indexEntry);
                BPlusTree tree = indexFromMetadata(metadata);

                // load data into index
                if (bulkLoad) {
                    throw new UnsupportedOperationException("not implemented");
                } else {
                    Table table = tableFromMetadata(tableMetadata);
                    for (RecordId rid : (Iterable<RecordId>) table::ridIterator) {
                        Record record = table.getRecord(rid);
                        tree.put(record.getValue(columnIndex), rid);
                    }
                }
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void dropIndex(String tableName, String columnName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                // We need exclusive write access on an index to drop it.
                LockUtil.ensureSufficientLockHeld(getColumnIndexMetadataContext(tableName, columnName), LockType.X);
                Pair<RecordId, BPlusTreeMetadata> pair = getColumnIndexMetadata(tableName, columnName);
                if (pair == null) {
                    throw new DatabaseException("no index on " + tableName + "(" + columnName + ")");
                }
                indexMetadata.deleteRecord(pair.getFirst());
                bufferManager.freePart(pair.getSecond().getPartNum());
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public QueryPlan query(String tableName) {
            return new QueryPlan(transactionContext, tableName);
        }

        @Override
        public QueryPlan query(String tableName, String alias) {
            return new QueryPlan(transactionContext, tableName, alias);
        }

        @Override
        public void insert(String tableName, Record values) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.addRecord(tableName, values);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue) {
            update(tableName, targetColumnName, targetValue, null, null, null);
        }

        @Override
        public void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue,
                           String predColumnName, PredicateOperator predOperator, DataBox predValue) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.updateRecordWhere(tableName, targetColumnName, targetValue, predColumnName,
                                                        predOperator, predValue);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void delete(String tableName, String predColumnName, PredicateOperator predOperator,
                           DataBox predValue) {
            TransactionContext.setTransaction(transactionContext);
            try {
                transactionContext.deleteRecordWhere(tableName, predColumnName, predOperator, predValue);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void savepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.savepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void rollbackToSavepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.rollbackToSavepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public void releaseSavepoint(String savepointName) {
            TransactionContext.setTransaction(transactionContext);
            try {
                recoveryManager.releaseSavepoint(transNum, savepointName);
            } finally {
                TransactionContext.unsetTransaction();
            }
        }

        @Override
        public Schema getSchema(String tableName) {
            return transactionContext.getSchema(tableName);
        }

        @Override
        public TransactionContext getTransactionContext() {
            return transactionContext;
        }

        @Override
        public String toString() {
            return "Transaction " + transNum + " (" + getStatus().toString() + ")";
        }
    }

    public void loadDemo() throws IOException {
        loadCSV("Students");
        loadCSV("Courses");
        loadCSV("Enrollments");
        waitAllTransactions();
        getBufferManager().evictAll();
    }

    /**
     * Loads a CSV from src/main/resources in as a table.
     * @param name the name of the csv file (without .csv extension)
     * @return true if the table already existed in the database, false otherwise
     */
    public boolean loadCSV(String name) throws IOException {
            InputStream is = Database.class.getClassLoader().getResourceAsStream(name + ".csv");
            InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
            BufferedReader buffered = new BufferedReader(reader);
            String[] header = buffered.readLine().split(",");
            Schema schema = new Schema();
            for (int i = 0; i < header.length; i++) {
                String[] parts = header[i].split(" ", 2);
                // Must have at least one space separating field and type
                assert parts.length == 2;
                String fieldName = parts[0];
                Type fieldType = Type.fromString(parts[1]);
                schema.add(fieldName, fieldType);
            }
            List<Record> rows = new ArrayList<>();
            String row = buffered.readLine();
            while (row != null) {
                String[] values = row.split(",");
                List<DataBox> parsed = new ArrayList<>();
                assert values.length == schema.size();
                for (int i = 0; i < values.length; i++) {
                    parsed.add(DataBox.fromString(schema.getFieldType(i), values[i]));
                }
                rows.add(new Record(parsed));
                row = buffered.readLine();
            }
            try(Transaction t = beginTransaction()) {
                t.createTable(schema, name);
            } catch (DatabaseException e) {
                if (e.getMessage().contains("already exists")) return true;
                throw e;
            }
            try (Transaction t = beginTransaction()) {
                for (Record r : rows) {
                    t.insert(name, r);
                }
            }
            return false;
    }
}
