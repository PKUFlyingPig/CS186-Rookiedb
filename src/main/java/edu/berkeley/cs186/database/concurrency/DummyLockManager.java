package edu.berkeley.cs186.database.concurrency;

import java.util.Collections;
import java.util.List;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

/**
 * Dummy lock manager that does no locking or error checking.
 *
 * Used for non-locking-related tests to disable locking.
 */
public class DummyLockManager extends LockManager {
    public DummyLockManager() { }

    @Override
    public LockContext context(String readable, long name) {
        return new DummyLockContext(new Pair<>(readable, name));
    }

    @Override
    public LockContext databaseContext() {
        return new DummyLockContext(new Pair<>("database", 0L));
    }

    @Override
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
    throws DuplicateLockRequestException, NoLockHeldException { }

    @Override
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException { }

    @Override
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException { }

    @Override
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException { }

    @Override
    public LockType getLockType(TransactionContext transaction, ResourceName name) {
        return LockType.NL;
    }

    @Override
    public List<Lock> getLocks(ResourceName name) {
        return Collections.emptyList();
    }

    @Override
    public List<Lock> getLocks(TransactionContext transaction) {
        return Collections.emptyList();
    }
}

