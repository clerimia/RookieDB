package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil 是一个声明层，它简化了多粒度锁获取的操作。
 * 通常来说，在锁获取时应该使用 LockUtil 而不是直接调用 LockContext 的方法。
 */
public class LockUtil {
    /**
     * 确保当前事务可以在 `lockContext` 上执行需要 `requestType` 锁类型的操作。
     *
     * `requestType` 保证是以下值之一：S、X、NL。
     *
     * 此方法应根据需要进行提升/升级/获取锁，但只应授予满足需求的最小权限锁集合。
     * 我们建议您考虑在以下每种情况下的处理方式：
     * - 当前锁类型可以有效替代请求类型
     * - 当前锁类型是 IX 并且请求锁是 S 锁
     * - 当前锁类型是一个意向锁
     * - 以上都不是：在这种情况下，考虑显式锁类型可以是什么值，
     *   并思考如何获取或更改祖先节点的锁。
     *
     * 您可能会发现创建一个辅助方法来确保您在所有祖先节点上都具有适当的锁会很有用。
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType 必须是 S, X, 或 NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // 如果事务或 lockContext 为空则不执行任何操作
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // 您可能会发现这些变量很有用
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): 实现
        return;
    }

    // TODO(proj4_part2) 添加您想要的任何辅助方法
}
