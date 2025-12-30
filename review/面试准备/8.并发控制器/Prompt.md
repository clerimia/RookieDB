并发控制层
1. 这是Project4 的内容，我负责实现一下内容
   - LockType
     - compatible(A, B): 判断两个不同事务对一个资源上的锁是否兼容
     - canBeParentLock(A, B): 判断一个事务上锁的时候是否可以作为另一个锁的父资源的锁
     - substitutable(substitute, required): 判断锁权限的可替代性
   - LockManager: 锁管理器, 也是锁的执行层, 实现了锁的等待队列
     - acquireAndRelease
     - acquire
     - release
     - promote
     - getLockType
   - LockContext: 多粒度锁的规则层
     - acquire
     - release
     - promote
     - escalate
     - getExplicitLockType
     - getEffectiveLockType
   - LockUtil: 锁管理器对其它层的接口，用于对特定的资源上 S/X 锁
     - ensureSufficientLockHeld
2. 这个部分实现了一个基于严格两阶段锁协议的多粒度锁
   - 最低粒度是页面粒度
3. 和MySQL等商业数据库的实现差别在哪?
4. 同样基于框架实现，没有全局视角