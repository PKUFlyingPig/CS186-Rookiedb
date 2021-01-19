package edu.berkeley.cs186.database.concurrency;

@SuppressWarnings("serial")
public class InvalidLockException extends RuntimeException {
    InvalidLockException(String message) {
        super(message);
    }
}

