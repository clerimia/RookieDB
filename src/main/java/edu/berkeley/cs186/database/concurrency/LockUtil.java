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
        // 1. 如果类型是NL，什么都不做
        if (requestType == LockType.NL) return;

        if (LockType.substitutable(effectiveLockType, requestType)) {
            // 2.1 如果当前锁权限已经满足要求了，什么都不做
            // S->S SIX->S X->X
            return;
        } else if (explicitLockType == LockType.IX && requestType == LockType.S) {
            // 2.2 当前锁类型是 IX 并且请求锁是 S 锁，需要升级为SIX
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            // 2.3 如果当前类型的锁是意向锁，
            // IS -> X 需要使用promote
            if (explicitLockType == LockType.IS && requestType == LockType.X) {
                lockContext.promote(transaction, LockType.X);
            } else {
                // IS -> S IX -> X SIX -> X直接 escalate
                lockContext.escalate(transaction);
            }
        } else {
            // 2.4 到这里仅有可能是 S -> X / SIX 或者压根没有锁 NL -> S/X
            // 确保祖先节点有权限
            ensureParentSufficientLockHeld(parentContext, transaction, requestType);
            // 这里已经满足需求了，并且做好了升级
            // 2.4.1 如果是S锁
            if (explicitLockType == LockType.S) {
                lockContext.promote(transaction, requestType);
            } else {
                // 2.4.1 如果是NL
                lockContext.acquire(transaction, requestType);
            }
        }

    }

    // TODO(proj4_part2) 添加您想要的任何辅助方法
    /**
     * 确保当前事务可以在上执行需要 `requestType` 锁类型的操作。
     *
     * 该方法遵循多粒度锁协议，确保从根节点到目标节点的路径上所有父节点都持有足够的锁权限。
     *
     * @param parentContext 当前上下文的父节点
     * @param transaction 事务
     * @param requestType 子节点需要的锁类型
     */
    private static void ensureParentSufficientLockHeld(LockContext parentContext, TransactionContext transaction, LockType requestType) {
        // base case: 如果父节点为空，直接返回
        if (parentContext == null) return;

        // 获取父节点上事务持有的显式锁类型
        LockType explicitLockType = parentContext.getExplicitLockType(transaction);

        // base case: 如果父节点的锁类型已经满足子节点的需求，直接返回
        if (LockType.canBeParentLock(explicitLockType, requestType)) {
            return;
        }

        // 计算父节点需要获取的锁类型
        LockType newLockType = LockType.parentLock(requestType);

        // base case: 如果没有祖父节点，直接在父节点上获取或升级锁
        if (parentContext.parentContext() == null) {
            if (explicitLockType == LockType.NL) {
                parentContext.acquire(transaction, newLockType);
            } else {
                parentContext.promote(transaction, newLockType);
            }
            return;
        }

        // recurse case: 递归确保祖父节点满足父节点的需求
        ensureParentSufficientLockHeld(parentContext.parentContext(), transaction, newLockType);

        // 然后处理父节点的锁需求
        if (explicitLockType == LockType.NL) {
            parentContext.acquire(transaction, newLockType);
        } else {
            // 这里有几种情况:
            // IS   ->  IX
            // S    ->  IX  ->  SIX
            if (explicitLockType == LockType.S && newLockType == LockType.IX) {
                parentContext.promote(transaction, LockType.SIX);
            } else {
                parentContext.promote(transaction, newLockType);
            }
        }
    }
}
