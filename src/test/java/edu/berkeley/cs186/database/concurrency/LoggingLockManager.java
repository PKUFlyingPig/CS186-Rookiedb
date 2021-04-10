package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingLockManager extends LockManager {
    public List<String> log = Collections.synchronizedList(new ArrayList<>());
    private boolean logging = false;
    private boolean suppressInternal = true;
    private boolean suppressStatus = false;
    private Map<String, LockContext> contexts = new HashMap<>();
    private Map<Long, Boolean> loggingOverride = new ConcurrentHashMap<>();

    @Override
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LoggingLockContext(this, null, name));
        }
        return contexts.get(name);
    }

    @Override
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames) {
        StringBuilder estr = new StringBuilder("acquire-and-release ");
        estr.append(transaction.getTransNum()).append(' ').append(name).append(' ').append(lockType);
        releaseNames.sort(Comparator.comparing(ResourceName::toString));
        estr.append(" [");
        boolean first = true;
        for (ResourceName n : releaseNames) {
            if (!first) {
                estr.append(", ");
            }
            estr.append(n);
            first = false;
        }
        estr.append(']');
        emit(estr.toString());

        Boolean[] oldOverride = new Boolean[1];
        loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> {
            oldOverride[0] = old;
            return !suppressInternal;
        });
        try {
            super.acquireAndRelease(transaction, name, lockType, releaseNames);
        } finally {
            loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> oldOverride[0]);
        }
    }

    @Override
    public void acquire(TransactionContext transaction, ResourceName name, LockType type) {
        emit("acquire " + transaction.getTransNum() + " " + name + " " + type);

        Boolean[] oldOverride = new Boolean[1];
        loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> {
            oldOverride[0] = old;
            return !suppressInternal;
        });
        try {
            super.acquire(transaction, name, type);
        } finally {
            loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> oldOverride[0]);
        }
    }

    @Override
    public void release(TransactionContext transaction, ResourceName name) {
        emit("release " + transaction.getTransNum() + " " + name);

        Boolean[] oldOverride = new Boolean[1];
        loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> {
            oldOverride[0] = old;
            return !suppressInternal;
        });
        try {
            super.release(transaction, name);
        } finally {
            loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> oldOverride[0]);
        }
    }

    @Override
    public void promote(TransactionContext transaction, ResourceName name, LockType newLockType) {
        emit("promote " + transaction.getTransNum() + " " + name + " " + newLockType);

        Boolean[] oldOverride = new Boolean[1];
        loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> {
            oldOverride[0] = old;
            return !suppressInternal;
        });
        try {
            super.promote(transaction, name, newLockType);
        } finally {
            loggingOverride.compute(Thread.currentThread().getId(), (id, old) -> oldOverride[0]);
        }
    }

    public void startLog() {
        logging = true;
    }

    public void endLog() {
        logging = false;
    }

    public void clearLog() {
        log.clear();
    }

    public boolean isLogging() {
        return logging;
    }

    void suppressInternals(boolean toggle) {
        suppressInternal = toggle;
    }

    public void suppressStatus(boolean toggle) {
        suppressStatus = toggle;
    }

    void emit(String s) {
        long tid = Thread.currentThread().getId();
        if (suppressStatus && !s.startsWith("acquire") && !s.startsWith("promote") &&
                !s.startsWith("release")) {
            return;
        }
        if ((loggingOverride.containsKey(tid) ? loggingOverride.get(tid) : logging)) {
            log.add(s);
        }
    }
}