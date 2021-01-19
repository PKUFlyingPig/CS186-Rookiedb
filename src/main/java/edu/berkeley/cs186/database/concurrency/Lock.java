package edu.berkeley.cs186.database.concurrency;

/**
 * Represents a lock held by a transaction on a resource.
 */
public class Lock {
    public ResourceName name;
    public LockType lockType;
    public Long transactionNum;

    public Lock(ResourceName name, LockType lockType, long transactionNum) {
        this.name = name;
        this.lockType = lockType;
        this.transactionNum = transactionNum;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        if (other == null) return false;
        if (!(other instanceof Lock)) return false;
        Lock l = (Lock) other;
        return name.equals(l.name) && lockType == l.lockType && transactionNum.equals(l.transactionNum);
    }

    @Override
    public int hashCode() {
        return 37 * (37 * name.hashCode() + lockType.hashCode()) + transactionNum.hashCode();
    }

    @Override
    public String toString() {
        return "T" + transactionNum.toString() + ": " + lockType.toString() + "(" + name.toString() + ")";
    }
}
