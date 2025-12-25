package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager维护事务对资源的锁定记录，并处理队列逻辑。通常不应直接使用锁管理器：
 * 而应通过调用LockContext的方法来 获取/释放/升级/提升锁。
 *
 * LockManager主要关注事务、资源和锁之间的映射关系，不处理多粒度级别。多粒度由LockContext处理。
 *
 * 锁管理器管理的每个资源都有自己的LockRequest对象队列，表示当时无法满足的获取（或升级/获取并释放）锁请求。
 * 每当该资源上的锁被释放时，都应处理此队列，从第一个请求开始，按顺序进行直到某个请求无法满足。
 * 从队列中取出的请求应被视为该事务在没有队列的情况下刚在资源释放后发出的请求
 * （即，移除T1获取X(db)的请求应被视为T1刚刚请求了X(db)，而db上没有队列：
 * T1应该获得db上的X锁，并通过Transaction#unblock置于非阻塞状态）。
 *
 * 这意味着在如下情况下：
 *    队列：S(A) X(A) S(A)
 * 当队列被处理时，只有第一个请求应从队列中移除。
 */
public class LockManager {
    // transactionLocks是从事务号到该事务持有的锁对象列表的映射。
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries是从资源名称到ResourceEntry对象的映射
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // ResourceEntry 包含资源上的锁列表以及对资源锁请求的队列(等待该资源的队列)。
    private class ResourceEntry {
        // 资源上当前授予的锁列表。
        List<Lock> locks = new ArrayList<>();
        // 此资源上尚未满足的锁请求队列。
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // 下面是我们建议您实现的一些辅助方法。
        // 您可以自由修改它们的类型签名、删除或忽略它们。

        /**
         * 检查`lockType`是否与现有锁兼容。允许与ID为`except`的事务持有的锁冲突，
         * 当事务尝试替换其已在资源上持有的锁时这很有用。
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): 实现
            for(Lock lock: locks){
                if (lock.transactionNum != except && !LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * 给事务授予锁`lock`。假设锁是兼容的。如果事务已持有锁，则更新资源上的锁。
         * 注意保持顺序一致
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): 实现
            // 找到锁在 资源锁列表 Index
            Long transactionNum = lock.transactionNum;
            int indexOfResource = -1, indexOfTransaction = -1;
            for (int i = 0; i < locks.size(); i++) {
                Lock oldLock = getLock(i);
                // 如果锁匹配了，就记录锁的下标
                if (Objects.equals(transactionNum, oldLock.transactionNum)) {
                    indexOfResource = i;
                    indexOfTransaction = getTransactionLockList(transactionNum).indexOf(oldLock);
                }
            }

            // 开始grant Or Update
            if (indexOfResource == -1) {
                // 如果没有旧锁，那就是赋予
                locks.add(lock);
                getTransactionLockList(transactionNum).add(lock);
            } else {
                // 如果有旧锁，那就是更新
                locks.set(indexOfResource, lock);
                getTransactionLockList(transactionNum).set(indexOfTransaction, lock);
            }
        }

        private Lock getLock(int i) {
            return locks.get(i);
        }

        /**
         * 释放锁`lock`并处理队列。假设锁之前已被授予。
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): 实现
            // 从资源 锁映射中移除锁
            locks.remove(lock);
            // 从事务中移除锁
            transactionLocks.get(lock.transactionNum).remove(lock);
            // 释放后处理等待的请求
            processQueue();
        }

        /**
         * 如果addFront为true，则将`request`添加到队列前面，否则添加到队列末尾。
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): 实现
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * 从队列的前面到后面授予锁，直到下一个锁无法授予为止。一旦请求完全被满足，
         * 发出请求的事务就可以解除阻塞。
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): 实现
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                Lock lock = request.lock;
                // 如果锁类型兼容就授予，让事务解除阻塞，从请求中移除
                if (this.checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    // 授予
                    this.grantOrUpdateLock(lock);
                    // 释放
                    releaseLocks(request.releasedLocks);
                    // 从请求中移除
                    requests.remove();
                    // 事务解除阻塞
                    request.transaction.unblock();
                } else {
                    // 锁类型不兼容，不能授予
                    break;
                }
            }
        }

        /**
         * 获取`transaction`在此资源上持有的锁类型。
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): 实现
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        public Lock getTransactionLock(long transaction) {
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock;
                }
            }

            return null;
        }

        @Override
        public String toString() {
            return "活跃锁: " + Arrays.toString(this.locks.toArray()) +
                    ", 队列: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // 您不应直接修改或使用此变量。
    // 仅使用context(String name)方法获取LockContext对象。
    // 代表了顶级资源锁层次树上下文，也就是数据库
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * 辅助方法，用于获取与`name`对应的resourceEntry。如果尚无条目，则在映射中插入
     * 一个新的（空的）resourceEntry。
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * 辅助方法，用于获取与`transactionNum`对应的锁列表
     * 如果尚无条目，则在映射中插入一个新的（空的）resourceEntry。
     * */
    private List<Lock> getTransactionLockList(Long transactionNum) {
        transactionLocks.putIfAbsent(transactionNum, new ArrayList<>());
        return transactionLocks.get(transactionNum);
    }

    /**
     * 辅助方法，用于将指定的锁列表中所有的锁释放
     * 这个方法假设这些锁已经都被授予了
     * */
    private void releaseLocks(List<Lock> locks) {
        for (Lock lock : locks) {
            ResourceEntry resourceEntry = getResourceEntry(lock.name);
            resourceEntry.releaseLock(lock);
        }
    }

    /**
     * 为事务`transaction`在`name`上获取`lockType`类型的锁，
     * 并在获取锁后以原子操作释放事务持有的所有`releaseNames`上的锁。
     *
     * 在获取或释放任何锁之前必须进行错误检查。
     * 如果新锁与另一个事务在资源上的锁不兼容，则事务被阻塞，请求被放置在资源队列的前面。
     *
     * 只有在获取请求的锁之后才应释放`releaseNames`上的锁。应处理相应的队列。
     *
     * 获取并释放旧的`name`上的锁不应更改`name`上锁的获取时间，
     * 即如果事务按以下顺序获取锁：S(A), X(B), 获取X(A)并释放S(A)，则认为A上的锁是在B上的锁之前获取的。
     *
     * @throws DuplicateLockRequestException 如果`transaction`已经持有`name`上的锁并且未被释放
     * @throws NoLockHeldException 如果`transaction`在`releaseNames`中的一个或多个名称上没有持有锁
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): 实现
        // 您可以修改此方法的任何部分。您不需要将所有代码保留在给定的同步块中，可
        // 以根据需要将同步块移动到其他位置。
        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType currentLock = resourceEntry.getTransactionLockType(transNum);
            // 处理第一种边界情况
            if (currentLock == lockType) {
                throw new DuplicateLockRequestException("Transaction " + transNum +
                        " 在资源上 " + name + " 已经有锁了，类型为： " + lockType);
            }
            // 处理第二种边界情况：判断能否进行锁删除，并获取事务在对应资源上的锁
            List<Lock> releaseLocks = new ArrayList<>();
            for (ResourceName releaseName : releaseNames) {
                ResourceEntry releaseEntry = getResourceEntry(releaseName);
                Lock lock = releaseEntry.getTransactionLock(transNum);
                if (lock == null) {
                    throw new NoLockHeldException("事务" + transNum + "在" + releaseName + "上没有获取锁");
                }
                // 如果是本资源就不需要删除了，等下可以grantOrUpdate
                if (releaseName == name) continue;
                releaseLocks.add(lock);
            }

            // 现在异常情况处理完毕，需要进行真正的获取和释放操作了
            // 1. 判断请求的锁是否兼容
            if (!resourceEntry.checkCompatible(lockType, transNum)) {
                // 如果不兼容，则请求失败，生成一个锁请求放在等待队列队首
                LockRequest lockRequest = new LockRequest(
                        transaction,
                        new Lock(name, lockType, transNum),
                        releaseLocks
                );
                resourceEntry.addToQueue(lockRequest, true);
                shouldBlock = true;
                // 预阻塞
                transaction.prepareBlock();
            } else {
                // 如果兼容，直接进行更改，然后执行释放操作
                resourceEntry.grantOrUpdateLock(new Lock(name, lockType, transNum));
                releaseLocks(releaseLocks);
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 为事务`transaction`在`name`上获取`lockType`类型的锁。
     *
     * 在获取锁之前必须进行错误检查。如果新锁与另一个事务在资源上的锁不兼容，或资源队列中有其他事务，则事务被阻塞，请求被放置在NAME队列的后面。
     *
     * @throws DuplicateLockRequestException 如果`transaction`已持有`name`上的锁
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): 实现
        // 您可以修改此方法的任何部分。您不需要将所有代码保留在给定的同步块中，可
        // 以根据需要将同步块移动到其他位置。
        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            // 获取资源Entry
            ResourceEntry entry = getResourceEntry(name);
            // 判断是否已经在对应资源上有了锁了
            LockType currentType = entry.getTransactionLockType(transNum);
            if (currentType  != LockType.NL) {
                // 如果已经有对应的锁了， 就抛出一个异常
                throw new DuplicateLockRequestException("Transaction " + transNum + "already holds a lock on " + name + " of type " +  currentType);
            }
            // 如果没有对应的锁，就判断是否兼容或者没有锁请求在等待
            if (!entry.checkCompatible(lockType, transNum) || !entry.waitingQueue.isEmpty()) {
                // 如果锁不兼容，或者还有锁请求在等待。就需要将当前请求包装成一个锁请求，然后把该请求放在队尾，然后需要阻塞
                shouldBlock = true;
                // 预阻塞，不能在同步块内部被阻塞。
                transaction.prepareBlock();
                entry.addToQueue(
                        new LockRequest(transaction, new Lock(name, lockType, transNum)),
                        false
                );
            } else {
                // 如果锁兼容，而且没有锁请求在等待了，就直接进行锁申请了
                entry.grantOrUpdateLock(new Lock(name, lockType, transNum));
            }

        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 释放`transaction`在`name`上的锁。在释放锁之前必须进行错误检查。
     *
     * 此调用后应处理资源名称的队列。如果队列中的任何请求有待释放的锁，则应释放这
     * 些锁，并处理相应的队列。
     *
     * @throws NoLockHeldException 如果`transaction`在`name`上没有持有锁
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): 实现
        // 您可以修改此方法的任何部分。
        // 先获取事务的事务ID
        long transNum = transaction.getTransNum();
        synchronized (this) {
            // 获取对应资源的Entry
            ResourceEntry resourceEntry = getResourceEntry(name);
            // 看entry上有没有对应事务的锁
            Lock lock = resourceEntry.getTransactionLock(transNum);
            if (lock == null) {
                // 如果没有，就抛出异常
                throw new NoLockHeldException("Transaction " + transNum + "在资源" + name + "没有持有锁");
            }
            // 如果有，就释放它
            // 先获取锁
            resourceEntry.releaseLock(lock);
        }
    }

    /**
     * 将事务在`name`上的锁升级为`newLockType`（即将事务在`name`上的当前锁类型更改为`newLockType`，如果是有效的替代）。
     *
     * 在更改任何锁之前必须进行错误检查。如果新锁与另一个事务在资源上的锁不兼容，则事务被阻塞，请求被放置在资源队列的前面。
     *
     * 锁升级不应更改锁的获取时间，即如果事务按以下顺序获取锁：S(A), X(B), 升级X(A)，
     * 则认为A上的锁是在B上的锁之前获取的。
     *
     * @throws DuplicateLockRequestException 如果`transaction`已经在`name`上持有`newLockType`类型的锁
     * @throws NoLockHeldException  如果`transaction`在`name`上没有持有锁
     * @throws InvalidLockException 如果请求的锁类型不是升级。从锁类型A到锁类型B的升级只有在B可以替代A且B不等于A时才是有效的。
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): 实现
        // 您可以修改此方法的任何部分。
        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            // 获取ResourceEntry
            ResourceEntry resourceEntry = getResourceEntry(name);
            // 获取资源上持有的锁
            Lock lock = resourceEntry.getTransactionLock(transNum);
            // 判断边界情况
            if (lock == null) {
                throw new NoLockHeldException("Transaction " + transNum + "在资源 " + name + "上没有锁");
            } else if (lock.lockType == newLockType) {
                throw new DuplicateLockRequestException("Transaction " + transNum + "在资源 " + name + "已经持有类型" + newLockType + "的锁");
            } else if (!LockType.substitutable(newLockType, lock.lockType)) {
                throw new InvalidLockException("Transaction " + transNum + "在资源 " + name + "上不能升级锁从" + lock.lockType + "到" + newLockType);
            }
            // 开始升级
            // 1. 检查更新是否冲突, 由于这个是高优先级更新，所以不用判断是否有等待队列
            if (!resourceEntry.checkCompatible(newLockType, transNum)) {
                // 如果锁不兼容，就创建一个锁请求放在等待队列前面
                shouldBlock = true;
                resourceEntry.addToQueue(
                        new LockRequest(transaction, new Lock(name, newLockType, transNum)),
                        true
                );
                transaction.prepareBlock();
            } else {
                // 如果兼容，直接更新就可以了
                resourceEntry.grantOrUpdateLock(new Lock(name, newLockType, transNum));
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 返回`transaction`在`name`上持有的锁类型，如果没有持有锁则返回NL。
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): 实现
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }


    /**
     * 返回在`name`上持有的锁列表，按获取顺序排列。
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * 返回由`transaction`持有的锁列表，按获取顺序排列。
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * 创建锁上下文。有关更多信息，请参见本文件顶部和LockContext.java顶部的注释。
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * 为数据库创建锁上下文。有关更多信息，请参见本文件顶部和LockContext.java顶部的注释。
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
