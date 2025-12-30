package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext包装了LockManager，提供了多粒度锁定的层次结构。
 * 对锁的获取/释放等调用应该主要通过LockContext完成，
 * 它提供了在层次结构中某一点访问锁定方法的能力（数据库、表X等）
 */
public class LockContext {
    // 您不应该删除这些字段。您可以根据需要添加额外的字段/方法。
    // 底层的锁管理器。
    protected final LockManager lockman;

    // 父级LockContext对象，如果此LockContext位于层次结构顶部则为null。
    protected final LockContext parent;

    // 此LockContext表示的资源名称。
    protected ResourceName name;

    // 此LockContext是否为只读。如果LockContext是只读的，
    // 则acquire/release/promote/escalate应抛出UnsupportedOperationException。
    protected boolean readonly;

    // 事务号与该事务持有的此LockContext子级锁数量之间的映射关系。
    protected final Map<Long, Integer> numChildLocks;

    // 您不应直接修改或使用此字段
    // 代表子层级
    protected final Map<String, LockContext> children;

    // 是否应将任何新的子级LockContext标记为只读。
    protected boolean childLocksDisabled;

    // 创建一个可写的
    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman,  // 锁管理器
                          LockContext parent,   // 在资源树中的父节点
                          String name,          // 资源名
                          boolean readonly      // 是否为只读
    ) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name); // 如果没有父节点，那么直接创建
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
                                                // 如果有父节点，那么继承父节点的层级
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>(); // 存储事务持有锁的子级数量
        this.children = new ConcurrentHashMap<>();      // 存储子级LockContext
        this.childLocksDisabled = readonly;             // 子级LockContext继承父级的访问read-only
    }

    /**
     * 创建一个LockContext，该LockContext表示资源树中的资源。
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);  // 创建根节点
        while (names.hasNext()) {   // 创建子级LockContext
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * 获取此锁上下文相关的资源名称。
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * 为事务'transaction'获取'lockType'类型的锁。
     *
     * 注意：您必须对numChildLocks进行必要的更新，否则对LockContext#getNumChildren的调用将无法正常工作。
     *
     * @throws InvalidLockException 如果请求无效
     * @throws DuplicateLockRequestException 如果事务已持有锁
     * @throws UnsupportedOperationException 如果上下文是只读的
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): 实现
        // 1. 进行多粒度锁协议约束限制
        // 1.1 如果节点只读，直接抛出异常
        if (readonly) throw new UnsupportedOperationException("资源 " + name + " 只读");
        // 1.2 如果事务在该节点上已经有锁了，直接抛出异常，这个负责当前节点的规则检查
        if (getExplicitLockType(transaction) != LockType.NL) throw new DuplicateLockRequestException("事务 " + transaction.getTransNum() + " 已经获取锁, 类型为" + lockType);
        // 1.3 检查上下文的约束，如果违反约束抛出异常
        // 如果申请的是NL锁，则直接抛出异常
        if (lockType == LockType.NL) throw new InvalidLockException("不允许锁序列: 申请的锁为NL");
        // 注意需要判断是否有上下文
        if (parent != null) {
            // 1.3.1 判断父节点的锁和当前节点将要申请的锁是否满足约束
            LockType parentExplicitLockType = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentExplicitLockType, lockType)) throw new InvalidLockException("不允许锁序列: 父锁" + parentExplicitLockType + " 子锁" + lockType);
            // 1.3.2 如果祖先有SIX锁，禁止获取IS/S锁，因为冗余
            if (hasSIXAncestor(transaction)
                    && (lockType == LockType.S || lockType == LockType.IS)) throw new InvalidLockException("不允许锁序列: 祖先有SIX锁，不允许获取IS/S锁");
        }

        // 2. 锁协议约束检查完毕，现在就可以调用底层锁执行层了
        lockman.acquire(transaction, name, lockType);
        // 3. 对numChildLocks进行必要的更新，这个需要对所有的父节点进行更新
        updateParentNumChildLocks(transaction, 1);
    }

    /**
     * 更新直接父节点直接子节点的锁数量
     *
     * @param transaction 事务
     * @param num 要增加或减少的锁数量（正数表示增加，负数表示减少）
     */
    private void updateParentNumChildLocks(TransactionContext transaction, int num) {
        if (parent != null) {
            long transNum = transaction.getTransNum();
            int numChildren = parent.getNumChildren(transaction);
            parent.numChildLocks.put(transNum, numChildren + num);
        }
    }


    /**
     * 释放'transaction'在'name'上的锁。
     *
     * 注意：您必须对numChildLocks进行必要的更新，否则对LockContext#getNumChildren的调用将无法正常工作。
     *
     * @throws NoLockHeldException 如果'transaction'未在'name'上持有锁
     * @throws InvalidLockException 如果无法释放锁，因为这样做会违反多粒度锁定约束
     * @throws UnsupportedOperationException 如果上下文是只读的
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): 实现
        // 1. 进行多粒度锁协议约束限制
        // 1.1 如果节点只读，直接抛出异常
        if (readonly) throw new UnsupportedOperationException("资源 " + name + " 只读");
        // 1.2 如果事务在该资源节点上没有获取显式锁，直接抛出异常
        if (getExplicitLockType(transaction) == LockType.NL) throw new NoLockHeldException("事务 " + transaction.getTransNum() + "在资源" + name + " 上没有获取锁");
        // 1.3 多粒度锁协议约束
        // 1.3.1 向上，如果说子节点还有锁，则抛出异常
        int numChildren = getNumChildren(transaction);
        if (numChildren != 0) throw new InvalidLockException("锁的释放必须从下到上， 这里子树上还有锁的数量为：" + numChildren);

        // 2. 调用锁执行层进行锁的释放
        lockman.release(transaction, name);

        // 3. 更新锁的数量
        updateParentNumChildLocks(transaction, -1);
    }

    /**
     * 将'transaction'的在此层级的锁升级到'newLockType'。对于从IS/IX升级到SIX，所有后代的S和IS锁必须同时释放。
     * 辅助函数sisDescendants可能在此处有用。
     *
     * 注意：您必须对numChildLocks进行必要的更新，否则对LockContext#getNumChildren的调用将无法正常工作。
     *
     * @throws DuplicateLockRequestException 如果'transaction'已经持有'newLockType'类型的锁
     * @throws NoLockHeldException 如果'transaction'没有锁
     * @throws InvalidLockException 如果请求的锁类型不是升级，或者升级会导致锁管理器进入无效状态
     * （例如IS(父级), X(子级)）。从锁类型A到锁类型B的升级是有效的，如果B可替代A且B不等于A，
     * 或者B是SIX且A是IS/IX/S，否则无效。hasSIXAncestor在此处可能有用。
     * @throws UnsupportedOperationException 如果上下文是只读的
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): 实现
        // 1. 进行多粒度锁协议约束限制
        // 1.1 如果节点只读，直接抛出异常
        if (readonly) throw new UnsupportedOperationException("资源 " + name + " 只读");
        // 1.2 如果事务在该资源节点上没有显式锁，直接抛出异常

        LockType oldLockType = getExplicitLockType(transaction);
        if (oldLockType == LockType.NL) throw new NoLockHeldException("事务 " + transaction.getTransNum() + "在资源" + name + " 上没有获取锁");
        // 1.3 判断是否这个更新符合要求
        // 1.3.1 如果已经有newLockType类型的锁了，抛出异常
        if (oldLockType == newLockType) throw new DuplicateLockRequestException("事务 " + transaction.getTransNum() + "在资源" + name + " 上已经获取相同类型的");
        // 1.3.2 如果这个请求不是升级，直接抛出异常
        if (!LockType.substitutable(newLockType, oldLockType)) throw new InvalidLockException(String.format("锁无法从 %s 升级到 %s", oldLockType, newLockType));
        // 1.3.3 考虑上下文
        if (newLockType == LockType.SIX && hasSIXAncestor(transaction)) throw new InvalidLockException((String.format("祖先节点已经有类型为SIX的锁了，无法再次申请")));

        // 2. 执行锁升级
        if (newLockType == LockType.SIX) {
            // 2.1 如果是升级到SIX，则需要释放子节点的S/IS锁，需要使用acquireAndRelease()
            // 获取此事务所有持有S/IS锁的子节点的列表
            List<ResourceName> releaseList = sisDescendants(transaction);
            releaseList.add(name);
            // 调用锁更新
            lockman.acquireAndRelease(transaction, name, newLockType, releaseList);
            // 更新所有被释放了锁的节点的numChildLocks
            for (ResourceName resourceName : releaseList) {
                // 只有是name的子节点才更新numChildLocks
                if (resourceName.isDescendantOf(name)) {
                    LockContext context = fromResourceName(lockman, resourceName);
                    context.updateParentNumChildLocks(transaction, -1);
                }
            }
        } else {
            // 直接升级
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * 将'transaction'从此上下文的后代升级到此级别的锁，使用S或X锁。
     * 此调用后不应有后代锁，并且在此调用之前对此上下文后代有效的每个操作都必须仍然有效。
     * 您应该只对锁管理器进行一次修改调用，并且应该只向锁管理器请求有关TRANSACTION的信息。
     *
     * 例如，如果一个事务有以下锁：
     *
     *                    IX(数据库)
     *                    /         \
     *               IX(表1)    S(表2)
     *                /      \
     *    S(表1页3)  X(表1页5)
     *
     * 那么在调用table1Context.escalate(transaction)之后，我们应该有：
     *
     *                    IX(数据库)
     *                    /         \
     *               X(表1)     S(表2)
     *
     * 如果事务持有的锁没有变化（例如连续多次调用escalate），则不应进行任何修改调用。
     *
     * 注意：您必须对所有相关上下文的numChildLocks进行必要的更新，否则对LockContext#getNumChildren
     * 的调用将无法正常工作。
     *
     * @throws NoLockHeldException 如果'transaction'在此级别没有锁
     * @throws UnsupportedOperationException 如果上下文是只读的
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): 实现
        // 这个调用只有可能是IS->S 或者IX->X 或者SIX->X
        // 1. 进行多粒度锁协议约束限制
        // 1.1 如果节点只读，直接抛出异常
        if (readonly) throw new UnsupportedOperationException("资源 " + name + " 只读");
        // 1.2 如果事务在该资源节点上没有锁，就抛出异常
        LockType lockType = getExplicitLockType(transaction);
        if (lockType == LockType.NL) {
            throw new NoLockHeldException("事务 " + transaction.getTransNum() + "在资源" + name + " 上没有获取锁");
        }

        // 2. 开始锁升级，注意这里只有可能是意向锁
        if (!(lockType == LockType.IS || lockType == LockType.IX || lockType == LockType.SIX)) {
            return;
        }
        // 2.1 收集所有有锁的子节点
        List<ResourceName> lockedDescendants = lockedDescendants(transaction);
        // 2.2 开始进行原子的锁升级
        if (lockType == LockType.IS) {
            lockedDescendants.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.S, lockedDescendants);
        } else if (lockType == LockType.IX || lockType == LockType.SIX) {
            lockedDescendants.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.X, lockedDescendants);
        }
        // 更新所有被释放了锁的节点的numChildLocks
        for (ResourceName resourceName : lockedDescendants) {
            if (resourceName.isDescendantOf(name)) {
                LockContext context = fromResourceName(lockman, resourceName);
                context.updateParentNumChildLocks(transaction, -1);
            }
        }
    }

    /**
     * 获取所有持有该事务的锁的资源
     * */
    private List<ResourceName> lockedDescendants(TransactionContext transaction) {
        // 1. base case: 如果该节点没有子节点了就直接返回空集合
        if (children.isEmpty()) {
            return Collections.emptyList();
        }
        // 2. recurse case: 这里就是递归情况了，检查直接子节点的锁情况，并直接加入对应的S / IS锁，然后递归加入子节点的子代
        List<ResourceName> lockedDescendants = new ArrayList<>();
        for (Map.Entry<String, LockContext> contextEntry : children.entrySet()) {
            String childName = contextEntry.getKey();
            LockContext childContext = contextEntry.getValue();
            // 2.1 创建子节点的ResourceName
            ResourceName childResourceName = new ResourceName(name, childName);
            // 2.2 查看事务在子节点上的锁类型
            LockType lockType = lockman.getLockType(transaction, childResourceName);
            // 2.3 如果不是空就加入
            if (lockType != LockType.NL) {
                lockedDescendants.add(childResourceName);
            }
            // 2.4 递归遍历子节点的子树
            lockedDescendants.addAll(childContext.lockedDescendants(transaction));
        }

        // 3. 返回
        return lockedDescendants;
    }

    /**
     * 获取'transaction'在此级别持有的锁类型，如果没有在此级别持有锁则返回NL。显式锁
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): 实现
        // 这个是显式锁，只用判断节点显示持有的锁
        return lockman.getLockType(transaction, name);
    }

    /**
     * 获取事务在此级别的锁类型，可能是隐式的（例如，高级别的显式S锁意味着此级别的S锁）
     * 或显式的。如果没有显式或隐式锁则返回NL。
     * 显式锁 + 等效锁
     *
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): 实现
        // 1. 先获取该节点的显式锁
        LockType explicitLockType = getExplicitLockType(transaction);

        // 2. 获取从父节点继承的隐式锁
        LockType implicitLockType = LockType.NL;
        if (parent != null) {
            LockType parentLockType = parent.getEffectiveLockType(transaction);
            // 从父节点的有效锁推导子节点的隐式锁
            if (parentLockType == LockType.X || parentLockType == LockType.S) {
                implicitLockType = parentLockType;
            } else if (parentLockType == LockType.SIX) {
                implicitLockType = LockType.S;
            }
        }

        // 3. 返回显式锁和隐式锁中权限更高的那个
        if (LockType.substitutable(explicitLockType, implicitLockType)) {
            return explicitLockType;
        } else {
            return implicitLockType;
        }
    }

    /**
     * 辅助方法，用于检查事务是否在此上下文的祖先处持有SIX锁
     * @param transaction 事务
     * @return 如果在祖先处持有SIX则返回true，否则返回false
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): 实现
        // 1. base case: 如果没有祖先了就返回false
        if (parent == null) {
            return false;
        }
        // 2. recurse case:
        // 2.1 先判断事务在父节点上有没有SIX锁 获取事务在父节点上的锁
        LockType lockType = parent.getExplicitLockType(transaction);
        // 2.2 如果是SIX锁返回 true
        if (lockType == LockType.SIX)  return true;
        // 2.3 如果不是SIX则递归
        return parent.hasSIXAncestor(transaction);
    }

    /**
     * 辅助方法，获取给定事务持有的所有S或IS锁的后代资源名称列表。
     * @param transaction 给定的事务
     * @return 事务持有S或IS锁的后代资源名称列表。
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): 实现
        // 1. base case: 如果该节点没有子节点了就直接返回空集合
        if (children.isEmpty()) {
            return Collections.emptyList();
        }
        // 2. recurse case: 这里就是递归情况了，检查直接子节点的锁情况，并直接加入对应的S / IS锁，然后递归加入子节点的子代
        List<ResourceName> sisDescendants = new ArrayList<>();
        for (Map.Entry<String, LockContext> contextEntry : children.entrySet()) {
            String childName = contextEntry.getKey();
            LockContext childContext = contextEntry.getValue();
            // 2.1 创建子节点的ResourceName
            ResourceName childResourceName = new ResourceName(name, childName);
            // 2.2 查看事务在子节点上的锁类型
            LockType lockType = lockman.getLockType(transaction, childResourceName);
            // 2.3 如果是S/IS加入
            if (lockType == LockType.S || lockType == LockType.IS) {
                sisDescendants.add(childResourceName);
            }
            // 2.4 递归遍历子节点的子树
            sisDescendants.addAll(childContext.sisDescendants(transaction));
        }

        // 3. 返回
        return sisDescendants;
    }

    /**
     * 禁用后代锁定。这会导致此上下文的所有新子上下文变为只读。
     * 这用于索引和临时表（我们不允许更细粒度的锁），前者由于锁定B+树的复杂性，
     * 后者由于临时表只对一个事务可访问，因此更细粒度的锁没有意义。
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * 获取父上下文。
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * 获取名称为'name'的子级上下文
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * 获取名称为'name'的子级上下文。
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * 获取单个事务在子级上持有的锁数量。
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

