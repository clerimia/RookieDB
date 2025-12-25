package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj4Part1Tests;
import edu.berkeley.cs186.database.categories.Proj4Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@Category({Proj4Tests.class, Proj4Part1Tests.class})
public class TestLockManager {
    private LoggingLockManager lockman;
    private TransactionContext[] transactions;
    private ResourceName dbResource;
    private ResourceName[] tables;

    // 每个测试2秒
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                2000 * TimeoutScaling.factor)));

    /**
     * 给定一个LockManager lockman，检查事务是否在指定名称的资源上持有指定类型的锁
     */
    static boolean holds(LockManager lockman, TransactionContext transaction, ResourceName name,
                         LockType type) {
        List<Lock> locks = lockman.getLocks(transaction);
        if (locks == null) {
            return false;
        }
        for (Lock lock : locks) {
            if (lock.name == name && lock.lockType == type) {
                return true;
            }
        }
        return false;
    }

    @Before
    public void setUp() {
        // 设置一个LockManager，8个可能的事务，一个数据库资源
        // 和8个用于测试的表
        lockman = new LoggingLockManager();
        transactions = new TransactionContext[8];
        dbResource = new ResourceName("database");
        tables = new ResourceName[transactions.length];
        for (int i = 0; i < transactions.length; ++i) {
            transactions[i] = new DummyTransactionContext(lockman, i);
            tables[i] = new ResourceName(dbResource,"table" + i);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLock() {
        /**
         * 事务0获取table0上的S锁
         * 事务1获取table1上的X锁
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(transactions[0], tables[0], LockType.S));
        runner.run(1, () -> lockman.acquire(transactions[1], tables[1], LockType.X));

        // 事务0应该在table0上有一个S锁
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[0]));

        // table0应该只从事务0有一个S锁
        List<Lock> expectedTable0Locks = Collections.singletonList(new Lock(tables[0], LockType.S, 0L));
        assertEquals(expectedTable0Locks, lockman.getLocks(tables[0]));

        // 事务1应该在table1上有一个X锁
        assertEquals(LockType.X, lockman.getLockType(transactions[1], tables[1]));

        // table1应该只从事务1有一个X锁
        List<Lock>expectedTable1Locks = Collections.singletonList(new Lock(tables[1], LockType.X, 1L));
        assertEquals(expectedTable1Locks, lockman.getLocks(tables[1]));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLockFail() {
        DeterministicRunner runner = new DeterministicRunner(1);
        TransactionContext t0 = transactions[0];

        // 事务0获取dbResource上的X锁
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
        try {
            // 事务0尝试在dbResource上获取另一个X锁
            runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
            fail("尝试获取重复锁应该抛出DuplicateLockRequestException。");
        } catch (DuplicateLockRequestException e) {
            // 不做任何操作
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleReleaseLock() {
        /**
         * 事务0获取dbResource上的X锁
         * 事务0释放其在dbResource上的锁
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquire(transactions[0], dbResource, LockType.X);
            lockman.release(transactions[0], dbResource);
        });

        // 事务0在dbResource上应该没有锁
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));

        // dbResource上应该没有锁
        assertEquals(Collections.emptyList(), lockman.getLocks(dbResource));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testReleaseUnheldLock() {
        DeterministicRunner runner = new DeterministicRunner(1);

        TransactionContext t1 = transactions[0];
        try {
            runner.run(0, () -> lockman.release(t1, dbResource));
            fail("释放你没有持有的资源上的锁应该抛出NoLockHeldException");
        } catch (NoLockHeldException e) {
            // 不做任何操作
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleConflict() {
        /**
         * 事务0获取dbResource上的X锁
         * 事务1尝试获取dbResource上的X锁，但由于与事务0的X锁冲突而被阻塞
         *
         * 在这之后：
         *   事务0应该在dbResource上有一个X锁
         *   事务1在dbResource上应该没有锁
         *   事务0不应该被阻塞
         *   事务1应该被阻塞（等待获取dbResource上的X锁）
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));

        // 锁检查
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.NL, lockman.getLockType(transactions[1], dbResource));
        List<Lock> expectedDbLocks = Collections.singletonList(new Lock(dbResource, LockType.X, 0L));
        assertEquals(expectedDbLocks, lockman.getLocks(dbResource));

        // 阻塞检查
        assertFalse(transactions[0].getBlocked());
        assertTrue(transactions[1].getBlocked());

        /**
         * 事务0释放其在dbResource上的锁
         * 事务1应该解除阻塞，并获取dbResource上的X锁
         *
         * 在这之后：
         *   事务0在dbResource上应该没有锁
         *   事务1应该在dbResource上有一个X锁
         *   两个事务都应该解除阻塞
         *
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // 锁检查
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.X, lockman.getLockType(transactions[1], dbResource));
        List<Lock> expectedDbLocks2 = Collections.singletonList(new Lock(dbResource, LockType.X, 1L));
        assertEquals(expectedDbLocks2, lockman.getLocks(dbResource));

        // 阻塞检查
        assertFalse(transactions[0].getBlocked());
        assertFalse(transactions[1].getBlocked());

        runner.joinAll();
    }

    // 以下测试不是Spring 2021 Project 4 Part 1截止日期所必需的。

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireRelease() {
        /**
         * 事务0获取table0上的S锁
         * 事务0获取table1上的S锁并释放其在table0上的锁
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Collections.emptyList());
            lockman.acquireAndRelease(transactions[0], tables[1], LockType.S, Collections.singletonList(tables[0]));
        });

        // 事务0在table0上应该没有锁
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], tables[0]));

        // table0上应该没有任何事务的锁
        assertEquals(Collections.emptyList(), lockman.getLocks(tables[0]));

        // 事务0应该在table1上有一个S锁
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[1]));

        // table1应该只从事务0有一个S锁
        List<Lock> expectedTable1Locks = Collections.singletonList(new Lock(tables[1], LockType.S, 0L));
        assertEquals(expectedTable1Locks, lockman.getLocks(tables[1]));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseQueue() {
        /**
         * 事务0获取table0上的X锁
         * 事务1获取table1上的X锁
         * 事务0尝试获取table1上的X锁并释放其在table0上的X锁
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        runner.run(1, () -> lockman.acquireAndRelease(transactions[1], tables[1], LockType.X,
                   Collections.emptyList()));
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[1], LockType.X,
                   Collections.singletonList(tables[0])));

        // 事务0应该在table0上有一个X锁
        assertEquals(LockType.X, lockman.getLockType(transactions[0], tables[0]));

        // table0应该只从事务0有一个X锁
        List<Lock> expectedTable0Locks = Collections.singletonList(new Lock(tables[0], LockType.X, 0L));
        assertEquals(expectedTable0Locks, lockman.getLocks(tables[0]));

        // 事务0在table1上应该没有锁
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], tables[1]));

        // table1应该只从事务1有一个X锁
        List<Lock> expectedTable1Locks = Collections.singletonList(new Lock(tables[1], LockType.X, 1L));
        assertEquals(expectedTable1Locks, lockman.getLocks(tables[1]));

        // 事务0应该仍然被阻塞等待获取table1上的X锁
        assertTrue(transactions[0].getBlocked());

        runner.join(1);
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseDuplicateLock() {
        // 事务0获取table0上的X锁
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        try {
            // 事务0尝试在table0上获取另一个X锁
            runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                       Collections.emptyList()));
            fail("尝试获取重复锁应该抛出DuplicateLockRequestException。");
        } catch (DuplicateLockRequestException e) {
            // 不做任何操作
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseNotHeld() {
        // 事务0获取table0上的X锁
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        try {
            // 事务0尝试在table2上获取X锁，
            // 并释放table0和table1上的锁。
            runner.run(0, () -> lockman.acquireAndRelease(
                transactions[0], tables[2],
                LockType.X, Arrays.asList(tables[0], tables[1]))
            );
            fail("尝试释放未持有的锁应该抛出NoLockHeldException。");
        } catch (NoLockHeldException e) {
            // 不做任何操作
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseUpgrade() {
        /**
         * 事务0获取table0上的S锁
         * 事务0获取table0上的X锁并释放其S锁
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Collections.emptyList());
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                                      Collections.singletonList(tables[0]));
        });

        // 事务0应该在table0上有一个X锁
        assertEquals(LockType.X, lockman.getLockType(transactions[0], tables[0]));

        // table0应该只从事务0有一个X锁
        List<Lock> expectedTable0Locks = Collections.singletonList(new Lock(tables[0], LockType.X, 0L));
        assertEquals(expectedTable0Locks, lockman.getLocks(tables[0]));

        // 事务0不应该被阻塞
        assertFalse(transactions[0].getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSXS() {
        /**
         * 事务0获取dbResource上的S锁
         * 事务1尝试获取dbResource上的X锁，但由于与事务0的S锁冲突而被阻塞
         * 事务2尝试获取dbResource上的S锁，但由于存在队列而被阻塞
         *
         * 在这之后：
         *    dbResource应该从事务0有一个S锁
         *    dbResource队列中应该有[X (T1), S (T2)]
         *    事务0应该不被阻塞
         *    事务1应该被阻塞
         *    事务2应该被阻塞
         */
        DeterministicRunner runner = new DeterministicRunner(3);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.S));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.S));

        // 锁检查
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 0L)),
                     lockman.getLocks(dbResource));

        // 阻塞检查
        List<Boolean> blocked_status = new ArrayList<>();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, true, true), blocked_status);

        /**
         * 事务0释放其在dbResource上的S锁
         * 事务1应该解除阻塞并获取dbResource上的X锁
         * 事务2将在队列中前进，但仍然被阻塞
         *
         * 在这之后：
         *    dbResource应该从事务1有一个X锁
         *    dbResource队列中应该有[S (T2)]
         *    事务0应该不被阻塞
         *    事务1应该不被阻塞
         *    事务2应该被阻塞
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // 锁检查
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 1L)),
                     lockman.getLocks(dbResource));

        // 阻塞检查
        blocked_status.clear();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, true), blocked_status);

        /**
         * 事务1释放其在dbResource上的X锁
         * 事务2应该解除阻塞并获取dbResource上的S锁
         *
         * 在这之后：
         *    dbResource应该从事务1有一个S锁
         *    所有事务都应该不被阻塞
         */
        runner.run(1, () -> lockman.release(transactions[1], dbResource));

        // 锁检查
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 2L)),
                     lockman.getLocks(dbResource));

        // 阻塞检查
        blocked_status.clear();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false), blocked_status);

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testXSXS() {
        /**
         * 事务0获取dbResource上的X锁
         * 事务1尝试获取dbResource上的S锁，但由于与事务0的X锁冲突而被阻塞
         * 事务2尝试获取dbResource上的X锁，但由于存在队列而被阻塞
         * 事务3尝试获取dbResource上的S锁，但由于存在队列而被阻塞
         *
         * 在这之后：
         *    事务0应该在dbResource上有一个X锁
         *    dbResource队列中应该有[S (T1), X (T2), S (T3)]
         *    事务0应该不被阻塞，其余事务应该被阻塞
         */
        DeterministicRunner runner = new DeterministicRunner(4);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.S));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.X));
        runner.run(3, () -> lockman.acquire(transactions[3], dbResource, LockType.S));

        // 锁检查
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 0L)),
                     lockman.getLocks(dbResource));

        // 阻塞检查
        List<Boolean> blocked_status = new ArrayList<>();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, true, true, true), blocked_status);

        /**
         * 事务0释放其在dbResource上的X锁
         * 事务1获取dbResource上的S锁并解除阻塞
         * 事务2在队列中前进但仍然被阻塞，因为与事务1的S锁冲突
         * 事务3在队列中前进但仍然在事务2后面被阻塞
         *
         * 在这之后：
         *    事务1应该在dbResource上有一个S锁
         *    dbResource队列中应该有[X (T2), S (T3)]
         *    事务0和1应该不被阻塞，2和3应该被阻塞
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // 锁检查
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 1L)),
                     lockman.getLocks(dbResource));

        // 阻塞检查
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, true, true), blocked_status);

        /**
         * 事务1释放其在dbResource上的S锁
         * 事务2获取dbResource上的X锁并解除阻塞
         * 事务3在队列中前进但仍然被阻塞，因为与事务2的X锁冲突
         *
         * 在这之后：
         *    事务2应该在dbResource上有一个X锁
         *    dbResource队列中应该有[S (T3)]
         *    事务0、1和2应该不被阻塞，3应该被阻塞
         */
        runner.run(1, () -> lockman.release(transactions[1], dbResource));

        // 锁检查
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 2L)),
                     lockman.getLocks(dbResource));

        // 阻塞检查
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false, true), blocked_status);

        /**
         * 事务2释放其在dbResource上的X锁
         * 事务3获取dbResource上的S锁并解除阻塞
         *
         * 在这之后：
         *    事务3应该在dbResource上有一个S锁
         *    dbResource队列应该为空
         *    所有事务都应该不被阻塞
         */
        runner.run(2, () -> lockman.release(transactions[2], dbResource));

        // 锁检查
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 3L)),
                     lockman.getLocks(dbResource));

        // 阻塞检查
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false, false), blocked_status);

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLock() {
        /**
         * 事务0获取dbResource上的S锁
         * 事务0将其在dbResource上的S锁升级为X锁
         */
        DeterministicRunner runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            lockman.acquire(transactions[0], dbResource, LockType.S);
            lockman.promote(transactions[0], dbResource, LockType.X);
        });

        // 锁检查
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 0L)),
                lockman.getLocks(dbResource));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLockNotHeld() {
        DeterministicRunner runner = new DeterministicRunner(1);
        try {
            runner.run(0, () -> lockman.promote(transactions[0], dbResource, LockType.X));
            fail("尝试升级一个不存在的锁应该抛出NoLockHeldException。");
        } catch (NoLockHeldException e) {
            // 不做任何操作
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLockAlreadyHeld() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        try {
            runner.run(0, () -> lockman.promote(transactions[0], dbResource, LockType.X));
            fail("尝试将锁升级到等效锁应该抛出DuplicateLockRequestException");
        } catch (DuplicateLockRequestException e) {
            // 不做任何操作
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testFIFOQueueLocks() {
        /**
         * 事务0获取dbResource上的X锁
         * 事务1尝试获取dbResource上的X锁，但由于与事务0的X锁冲突而被阻塞
         * 事务2尝试获取dbResource上的X锁，但由于存在队列而被阻塞
         *
         * 在这之后：
         *    事务0应该在dbResource上有一个X锁
         *    dbResource队列中应该有[X (T1), X (T2)]
         */
        DeterministicRunner runner = new DeterministicRunner(3);
        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.X));

        // 锁检查
        assertTrue(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        /**
         * 事务0释放其在dbResource上的X锁
         * 事务1获取dbResource上的X锁并解除阻塞
         * 事务2在队列中前进但仍然被阻塞，因为与事务1的X锁冲突
         *
         * 在这之后：
         *    事务1应该在dbResource上有一个X锁
         *    dbResource队列中应该有[X (T2)]
         */
        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        // 锁检查
        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        /**
         * 事务1释放其在dbResource上的X锁
         * 事务2获取dbResource上的X锁并解除阻塞
         *
         * 在这之后：
         *    事务2应该在dbResource上有一个X锁
         */
        runner.run(1, () -> lockman.release(transactions[1], dbResource));

        // 锁检查
        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[2], dbResource, LockType.X));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testStatusUpdates() {
        TransactionContext t0 = transactions[0];
        TransactionContext t1 = transactions[1];

        /**
         * 事务0获取dbResource上的X锁
         * 事务1尝试获取dbResource上的X锁，但由于与事务0的X锁冲突而被阻塞
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(t1, dbResource, LockType.X));

        // 锁检查
        assertTrue(holds(lockman, t0, dbResource, LockType.X));
        assertFalse(holds(lockman, t1, dbResource, LockType.X));

        // 阻塞检查
        assertFalse(t0.getBlocked());
        assertTrue(t1.getBlocked());

        /**
         * 事务0释放其在dbResource上的X锁
         * 事务1获取dbResource上的X锁并解除阻塞
         */
        runner.run(0, () -> lockman.release(t0, dbResource));

        // 锁检查
        assertFalse(holds(lockman, t0, dbResource, LockType.X));
        assertTrue(holds(lockman, t1, dbResource, LockType.X));

        // 阻塞检查
        assertFalse(t0.getBlocked());
        assertFalse(t1.getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testTableEventualUpgrade() {
        TransactionContext t0 = transactions[0];
        TransactionContext t1 = transactions[1];

        /**
         * 事务0获取dbResource上的S锁
         * 事务1获取dbResource上的S锁
         */
        DeterministicRunner runner = new DeterministicRunner(2);
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.S));
        runner.run(1, () -> lockman.acquire(t1, dbResource, LockType.S));

        assertTrue(holds(lockman, t0, dbResource, LockType.S));
        assertTrue(holds(lockman, t1, dbResource, LockType.S));

        /**
         * 事务0尝试将其S锁升级为X锁，但由于与事务1的S锁冲突而失败。事务0
         * 将X锁加入队列并被阻塞。
         */
        runner.run(0, () -> lockman.promote(t0, dbResource, LockType.X));

        assertTrue(holds(lockman, t0, dbResource, LockType.S));
        assertFalse(holds(lockman, t0, dbResource, LockType.X));
        assertTrue(holds(lockman, t1, dbResource, LockType.S));
        assertTrue(t0.getBlocked());

        /**
         * 事务1释放其在dbResource上的S锁
         * 事务0将其S锁升级为X锁并解除阻塞
         */
        runner.run(1, () -> lockman.release(t1, dbResource));

        assertTrue(holds(lockman, t0, dbResource, LockType.X));
        assertFalse(holds(lockman, t1, dbResource, LockType.S));
        assertFalse(t0.getBlocked());

        /**
         * 事务0释放其在dbResource上的X锁
         */
        runner.run(0, () -> lockman.release(t0, dbResource));

        assertFalse(holds(lockman, t0, dbResource, LockType.X));
        assertFalse(holds(lockman, t1, dbResource, LockType.S));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testIntentBlockedAcquire() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t0 = transactions[0];
        TransactionContext t1 = transactions[1];

        /**
         * 事务0获取dbResource上的S锁
         * 事务1尝试获取dbResource上的IX锁，但由于与事务0的S锁冲突而被阻塞
         */
        runner.run(0, () -> lockman.acquire(t0, dbResource, LockType.S));
        runner.run(1, () -> lockman.acquire(t1, dbResource, LockType.IX));
        assertFalse(t0.getBlocked());
        assertTrue(t1.getBlocked());

        // 锁检查
        assertTrue(holds(lockman, t0, dbResource, LockType.S));
        assertFalse(holds(lockman, t1, dbResource, LockType.IX));

        /**
         * 事务0释放其在dbResource上的S锁
         * 事务1获取dbResource上的IX锁并解除阻塞
         */
        runner.run(0, () -> lockman.release(t0, dbResource));

        assertFalse(holds(lockman, t0, dbResource, LockType.S));
        assertTrue(holds(lockman, t1, dbResource, LockType.IX));
        assertFalse(t0.getBlocked());
        assertFalse(t1.getBlocked());

        runner.joinAll();
    }

}

