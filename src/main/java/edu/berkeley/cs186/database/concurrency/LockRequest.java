package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Collections;
import java.util.List;

/**
 * 表示事务队列中的一个锁请求，为`transaction`请求`lock`锁，
 * 并释放`releasedLocks`中的所有锁。应当在事务解除阻塞之前，
 * 先授予`lock`锁并释放`releasedLocks`中的所有锁。
 */
class LockRequest {
    TransactionContext transaction;
    Lock lock;
    List<Lock> releasedLocks;

    // 请求`lock`锁的锁请求，不释放任何锁。
    LockRequest(TransactionContext transaction, Lock lock) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = Collections.emptyList();
    }

    // 请求`lock`锁的锁请求，用以交换`releasedLocks`中的所有锁。
    LockRequest(TransactionContext transaction, Lock lock, List<Lock> releasedLocks) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = releasedLocks;
    }

    @Override
    public String toString() {
        return "Request for " + lock.toString() + " (releasing " + releasedLocks.toString() + ")";
    }
}