package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Collections;
import java.util.List;

/**
 * Dummy lock manager that does no locking or error checking.
 *
 * Used for non-locking-related tests to disable locking. This allows earlier
 * and later projects to be completed without needing to complete Project 4.
 */
public class DummyLockManager extends LockManager {
    public DummyLockManager() { }

    @Override
    public LockContext context(String name) {
        return new DummyLockContext(name);
    }

    @Override
    public LockContext databaseContext() {
        return new DummyLockContext("database");
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

