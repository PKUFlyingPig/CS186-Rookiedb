package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

/**
 * A lock context that doesn't do anything at all. Used where a lock context
 * is expected, but no locking should be done.
 */
public class DummyLockContext extends LockContext {
    public DummyLockContext() {
        this((LockContext) null);
    }

    public DummyLockContext(LockContext parent) {
        super(new DummyLockManager(), parent, new Pair<>("Unnamed", -1L));
    }

    public DummyLockContext(Pair<String, Long> name) {
        this(null, name);
    }

    public DummyLockContext(LockContext parent, Pair<String, Long> name) {
        super(new DummyLockManager(), parent, name);
    }

    @Override
    public void acquire(TransactionContext transaction, LockType lockType) { }

    @Override
    public void release(TransactionContext transaction) { }

    @Override
    public void promote(TransactionContext transaction, LockType newLockType) { }

    @Override
    public void escalate(TransactionContext transaction) { }

    @Override
    public void disableChildLocks() { }

    @Override
    public LockContext childContext(String readable, long name) {
        return new DummyLockContext(this, new Pair<>(readable, name));
    }

    @Override
    public int getNumChildren(TransactionContext transaction) {
        return 0;
    }

    @Override
    public LockType getExplicitLockType(TransactionContext transaction) {
        return LockType.NL;
    }

    @Override
    public LockType getEffectiveLockType(TransactionContext transaction) {
        return LockType.NL;
    }

    @Override
    public String toString() {
        return "Dummy Lock Context(\"" + name.toString() + "\")";
    }
}

