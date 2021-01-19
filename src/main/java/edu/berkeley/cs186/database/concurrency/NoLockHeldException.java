package edu.berkeley.cs186.database.concurrency;

@SuppressWarnings("serial")
public class NoLockHeldException extends RuntimeException {
    NoLockHeldException(String message) {
        super(message);
    }
}

