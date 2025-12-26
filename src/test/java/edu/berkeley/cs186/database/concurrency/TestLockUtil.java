package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({Proj4Tests.class, Proj4Part2Tests.class})
public class TestLockUtil {
    private LoggingLockManager lockManager;
    private TransactionContext transaction;
    private LockContext dbContext;
    private LockContext tableContext;
    private LockContext[] pageContexts;

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            1000 * TimeoutScaling.factor)));

    @Before
    public void setUp() {
        /**
         * For all of these tests we have the following resource hierarchy
         *                     database
         *                        |
         *                      table1
         *         _____________/ / \ \_______________
         *       /     /     /   /   \    \     \     \
         *  page0 page1 page2 page3 page4 page5 page6 page7
         */
        lockManager = new LoggingLockManager();
        transaction = new DummyTransactionContext(lockManager, 0);
        dbContext = lockManager.databaseContext();
        tableContext = dbContext.childContext("table1");
        pageContexts = new LockContext[8];
        for (int i = 0; i < pageContexts.length; ++i) {
            pageContexts[i] = tableContext.childContext((long) i);
        }
        TransactionContext.setTransaction(transaction);
    }

    @Test
    @Category(SystemTests.class)
    public void testRequestNullTransaction() {
        /**
         * 如果当前没有事务，则对 ensureSufficientLockHeld 的调用应该什么都不做。
         */
        lockManager.startLog();
        // Unset the transaction set by setUp()
        TransactionContext.unsetTransaction();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Collections.emptyList(), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquire() {
        /**
         * 请求页面4的S锁应该在祖先节点上获取正确的锁
         * （数据库上的IS锁，table1上的IS锁）并授予页面4上的S锁。
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/4 S"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromote() {
        /**
         * 请求页面4的S锁应该在数据库上获得IS锁，在table1上获得IS锁，
         * 并在页面4上授予S锁。
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/4 S"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * 之后，在页面4上请求X锁应该将数据库和table1上的IS锁提升为IX，
         * 并将页面4上的S锁提升为X。
         */
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.X);
        assertEquals(Arrays.asList(
                "promote 0 database IX",
                "promote 0 database/table1 IX",
                "promote 0 database/table1/4 X"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testIStoS() {
        /**
         * 与前两个测试一样，我们首先请求对页面4的S锁，然后释放页面4上的锁。
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        pageContexts[4].release(transaction);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/4 S",
                "release 0 database/table1/4"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * 之后我们应该有 IS(database) 和 IS(table1)。
         * 当我们在 table1 上请求 S 锁时，我们应该释放 table1 上的 IS 锁，
         * 并使用 acquire-and-release 在其位置获取 S 锁。
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                "acquire-and-release 0 database/table1 S [database/table1]"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleEscalate() {
        /**
         * 我们首先像前三个测试一样请求页面4上的S锁
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/4 S"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * 在表上升级锁应该释放 IS(table1) 和 S(page 4) 并获取 S(table1)。
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                "acquire-and-release 0 database/table1 S [database/table1, database/table1/4]"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testIXBeforeIS() {
        /**
         * 我们首先请求对页面3的X锁，这应该导致你在数据库上获得IX锁，在table1上也获得IX锁。
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);;
        assertEquals(Arrays.asList(
                "acquire 0 database IX",
                "acquire 0 database/table1 IX",
                "acquire 0 database/table1/3 X"
        ), lockManager.log);
        lockManager.clearLog();


        /**
         * 之后，在页面4上请求S锁应该可以使用table1上已有的IX锁，
         * 而不需要任何其他获取操作。
         *
         *          IX(数据库)
         *                |
         *           IX(表1)
         *             /     \
         *        X(页面3) S(页面4)
         */
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Collections.singletonList(
                "acquire 0 database/table1/4 S"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSIX1() {
        /**
         * 我们首先请求对页面3的X锁，这应该导致你在数据库上获得IX锁，在table1上也获得IX锁。
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);
        assertEquals(Arrays.asList(
                "acquire 0 database IX",
                "acquire 0 database/table1 IX",
                "acquire 0 database/table1/3 X"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * 之后，在table1上请求S锁应该将table1的IX锁提升为SIX。
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                "acquire-and-release 0 database/table1 SIX [database/table1]"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSIX2() {
        /**
         * 我们在page1上请求S锁，在page2上请求S锁，以及在page3上请求X锁。这应该给我们
         * 如下的结构：
         *
         *          IX(数据库)
         *                |
         *           IX(表1)
         *          /     |    \
         *   S(page1) S(page2) X(page3)
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[1], LockType.S);
        LockUtil.ensureSufficientLockHeld(pageContexts[2], LockType.S);
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);
        assertEquals(Arrays.asList(
                "acquire 0 database IS",
                "acquire 0 database/table1 IS",
                "acquire 0 database/table1/1 S",
                "acquire 0 database/table1/2 S",
                "promote 0 database IX",
                "promote 0 database/table1 IX",
                "acquire 0 database/table1/3 X"
        ), lockManager.log);
        lockManager.clearLog();

        /**
         * 之后我们在table1上请求S锁。由于table1当前持有IX锁，
         * 这应该将其提升为SIX。记住，提升到SIX应该释放任何IS/S的后代锁，在这种情况下是页面1和2上的锁
         *
         *            IX(数据库)
         *                |
         *           SIX(table1)
         *          /     |    \
         *   NL(page1) NL(page2) X(page3)
         */
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                "acquire-and-release 0 database/table1 SIX [database/table1, database/table1/1, database/table1/2]"
        ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleNL() {
        /**
         * Requesting NL should do nothing.
         */
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.NL);
        assertEquals(Collections.emptyList(), lockManager.log);
    }

}

