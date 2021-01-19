package edu.berkeley.cs186.database.concurrency;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A utility class for running code over multiple threads, in a specific order. Any
 * exceptions/errors thrown in a child thread are rethrown in the main thread.
 */
public class DeterministicRunner {
    private final Worker[] workers;
    private Throwable error = null;

    private class Worker implements Runnable {
        private Thread thread;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition sleepCondition = lock.newCondition();
        private final Condition wakeCondition = lock.newCondition();
        private final AtomicBoolean awake = new AtomicBoolean(false);
        private final AtomicBoolean ready = new AtomicBoolean(false);
        private Runnable nextTask = null;

        public Worker() {
            this.thread = new Thread(this);
        }

        @Override
        public void run() {
            try {
                sleep();
                while (nextTask != null) {
                    nextTask.run();
                    sleep();
                }
            } catch (Throwable throwable) {
                error = throwable;
            }
        }

        private void sleep() {
            lock.lock();
            try {
                while (!ready.get()) {
                    sleepCondition.awaitUninterruptibly();
                }
            } finally {
                awake.set(true);
                ready.set(false);
                wakeCondition.signal();
                lock.unlock();
            }
        }

        public void start() {
            thread.start();
        }

        public void runTask(Runnable next) {
            lock.lock();
            try {
                nextTask = next;
                ready.set(true);
                sleepCondition.signal();
            } finally {
                lock.unlock();
            }
            lock.lock();
            try {
                while (!awake.get()) {
                    wakeCondition.awaitUninterruptibly();
                }
                awake.set(false);
            } finally {
                lock.unlock();
            }
            while (thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.TERMINATED &&
                    error == null) {
                // return when either we finished the task (and went back to sleep)
                // or when the task caused the thread to block
                Thread.yield();
            }
        }

        public void join() throws InterruptedException {
            lock.lock();
            try {
                nextTask = null;
                ready.set(true);
                sleepCondition.signal();
            } finally {
                lock.unlock();
            }
            thread.join();
        }
    }

    public DeterministicRunner(int numWorkers) {
        this.workers = new Worker[numWorkers];
        for (int i = 0; i < numWorkers; ++i) {
            this.workers[i] = new Worker();
            this.workers[i].start();
        }
    }

    public void run(int thread, Runnable task) {
        error = null;
        this.workers[thread].runTask(task);
        if (error != null) {
            rethrow(error);
        }
    }

    public void join(int thread) {
        try {
            this.workers[thread].join();
        } catch (Throwable t) {
            rethrow(t);
        }
    }

    public void joinAll() {
        for (int i = 0; i < this.workers.length; ++i) {
            join(i);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void rethrow(Throwable t) throws T {
        // rethrows checked exceptions as unchecked
        throw (T) t;
    }

}
