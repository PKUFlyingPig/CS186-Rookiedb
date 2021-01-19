package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;

import java.util.HashMap;
import java.util.Map;

public class DummyRecoveryManager implements RecoveryManager {
    private Map<Long, Transaction> runningTransactions = new HashMap<>();

    @Override
    public void initialize() {}

    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {}

    @Override
    public void startTransaction(Transaction transaction) {
        runningTransactions.put(transaction.getTransNum(), transaction);
    }

    @Override
    public long commit(long transNum) {
        runningTransactions.get(transNum).setStatus(Transaction.Status.COMMITTING);
        return 0L;
    }

    @Override
    public long abort(long transNum) {
        throw new UnsupportedOperationException("proj5 must be implemented to use abort");
    }

    @Override
    public long end(long transNum) {
        runningTransactions.get(transNum).setStatus(Transaction.Status.COMPLETE);
        runningTransactions.remove(transNum);
        return 0L;
    }

    @Override
    public void pageFlushHook(long pageLSN) {}

    @Override
    public void diskIOHook(long pageNum) {}

    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        return 0L;
    }

    @Override
    public long logAllocPart(long transNum, int partNum) {
        return 0L;
    }

    @Override
    public long logFreePart(long transNum, int partNum) {
        return 0L;
    }

    @Override
    public long logAllocPage(long transNum, long pageNum) {
        return 0L;
    }

    @Override
    public long logFreePage(long transNum, long pageNum) {
        return 0L;
    }

    @Override
    public void savepoint(long transNum, String name) {
        throw new UnsupportedOperationException("proj5 must be implemented to use savepoints");
    }

    @Override
    public void releaseSavepoint(long transNum, String name) {
        throw new UnsupportedOperationException("proj5 must be implemented to use savepoints");
    }

    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        throw new UnsupportedOperationException("proj5 must be implemented to use savepoints");
    }

    @Override
    public void checkpoint() {
        throw new UnsupportedOperationException("proj5 must be implemented to use checkpoints");
    }

    @Override
    public Runnable restart() {
        return () -> {};
    }

    @Override
    public void close() {}
}
