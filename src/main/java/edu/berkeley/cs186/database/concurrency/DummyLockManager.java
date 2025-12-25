package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Collections;
import java.util.List;

/**
 * 虚拟锁管理器，不执行任何锁定或错误检查。
 *
 * 用于与锁定无关的测试，以禁用锁定功能。这使得早期和后期项目可以在不完成项目4的情况下完成。
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

