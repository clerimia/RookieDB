package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * 一个什么都不做的锁上下文。用在需要锁上下文，
 * 但不需要进行加锁的地方。
 *
 * 一个有用的例子：临时表（例如外部排序中创建的运行表）
 * 只能由创建它们的事务访问。由于多个事务同时尝试访问
 * 这些表的可能性为零，我们可以安全地使用虚拟锁上下文，
 * 因为不需要跨事务同步。
 */
public class DummyLockContext extends LockContext {
    public DummyLockContext() {
        this((LockContext) null);
    }

    public DummyLockContext(LockContext parent) {
        super(new DummyLockManager(), parent, "Unnamed");
    }

    public DummyLockContext(String name) {
        this(null, name);
    }

    public DummyLockContext(LockContext parent, String name) {
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
    public LockContext childContext(String name) {
        return new DummyLockContext(this, name);
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

