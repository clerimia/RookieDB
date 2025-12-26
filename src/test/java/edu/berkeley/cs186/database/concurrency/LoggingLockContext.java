package edu.berkeley.cs186.database.concurrency;

public class LoggingLockContext extends LockContext {
    private boolean allowDisable = true;

    /**
     * 一个特殊的 LockContext，与 LoggingLockManager 配合使用，在用户调用 disableChildLocks() 时输出日志
     */
    LoggingLockContext(LoggingLockManager lockman, LockContext parent, String name) {
        super(lockman, parent, name);
    }

    private LoggingLockContext(LoggingLockManager lockman, LockContext parent, String name,
                               boolean readonly) {
        super(lockman, parent, name, readonly);
    }

    /**
     * 禁用子级锁定。这会导致此上下文的所有子上下文变为只读。
     * 这用于索引和临时表（在这些地方我们不允许更细粒度的锁），
     * 前者是因为 B+ 树锁定的复杂性，后者是因为临时表只能被一个事务访问，
     * 所以更细粒度的锁没有意义。
     */
    @Override
    public synchronized void disableChildLocks() {
        if (this.allowDisable) {
            super.disableChildLocks();
        }
        ((LoggingLockManager) lockman).emit("disable-children " + name);
    }

    /**
     * 获取名称为 NAME 的子上下文（具有可读版本 READABLE）。
     */
    @Override
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LoggingLockContext((LoggingLockManager) lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        return child;
    }

    public synchronized void allowDisableChildLocks(boolean allow) {
        this.allowDisable = allow;
    }
}

