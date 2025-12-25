package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category({Proj4Tests.class, Proj4Part1Tests.class})
public class TestLockType {
    // 每个测试200毫秒
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            200 * TimeoutScaling.factor)));

    /**
     * 兼容性矩阵
     * (单元格中的布尔值回答'左边的锁'是否与'顶部的锁'兼容？)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  T  |  T  |     |
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  F  |     |
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |     |     |  F  |     |
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |     |     |  F  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     *
     * 已填充的单元格由公共测试覆盖。
     * 你可以期待空白单元格由隐藏测试覆盖！
     * 提示：我打赌笔记中可能有一些有用的内容...
     */

    @Test
    @Category(PublicTests.class)
    public void testCompatibleNL() {
        // NL 应该与每种锁类型兼容
        assertTrue(LockType.compatible(LockType.NL, LockType.NL));
        assertTrue(LockType.compatible(LockType.NL, LockType.S));
        assertTrue(LockType.compatible(LockType.NL, LockType.X));
        assertTrue(LockType.compatible(LockType.NL, LockType.IS));
        assertTrue(LockType.compatible(LockType.NL, LockType.IX));
        assertTrue(LockType.compatible(LockType.NL, LockType.SIX));
        assertTrue(LockType.compatible(LockType.S, LockType.NL));
        assertTrue(LockType.compatible(LockType.X, LockType.NL));
        assertTrue(LockType.compatible(LockType.IS, LockType.NL));
        assertTrue(LockType.compatible(LockType.IX, LockType.NL));
        assertTrue(LockType.compatible(LockType.SIX, LockType.NL));
    }

    @Test
    @Category(PublicTests.class)
    public void testCompatibleS() {
        // S 与 S 和 IS 兼容
        assertTrue(LockType.compatible(LockType.S, LockType.S));
        assertTrue(LockType.compatible(LockType.S, LockType.IS));
        assertTrue(LockType.compatible(LockType.IS, LockType.S));

        // S 与 X、IX 和 SIX 不兼容
        assertFalse(LockType.compatible(LockType.S, LockType.X));
        assertFalse(LockType.compatible(LockType.S, LockType.IX));
        assertFalse(LockType.compatible(LockType.S, LockType.SIX));
        assertFalse(LockType.compatible(LockType.X, LockType.S));
        assertFalse(LockType.compatible(LockType.IX, LockType.S));
        assertFalse(LockType.compatible(LockType.SIX, LockType.S));
    }

    @Test
    @Category(PublicTests.class)
    public void testCompatibleIntent() {
        // 意图锁相互兼容
        assertTrue(LockType.compatible(LockType.IS, LockType.IS));
        assertTrue(LockType.compatible(LockType.IS, LockType.IX));
        assertTrue(LockType.compatible(LockType.IX, LockType.IS));
        assertTrue(LockType.compatible(LockType.IX, LockType.IX));
    }

    @Test
    @Category(PublicTests.class)
    public void testCompatibleXandX() {
        // X 锁与 X 锁不兼容
        assertFalse(LockType.compatible(LockType.X, LockType.X));
    }

    @Test
    @Category(SystemTests.class)
    public void testParent() {
        // 这是一个对 LockType.parentLock 的详尽测试
        // 针对有效的锁类型
        assertEquals(LockType.NL, LockType.parentLock(LockType.NL));
        assertEquals(LockType.IS, LockType.parentLock(LockType.S));
        assertEquals(LockType.IX, LockType.parentLock(LockType.X));
        assertEquals(LockType.IS, LockType.parentLock(LockType.IS));
        assertEquals(LockType.IX, LockType.parentLock(LockType.IX));
        assertEquals(LockType.IX, LockType.parentLock(LockType.SIX));
    }

    /**
     * 父锁矩阵
     * (单元格中的布尔值回答'左边的锁'是否可以是'顶部的锁'的父锁？)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |     |     |     |     |
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |     |     |     |     |
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |     |     |     |     |
     * ----+-----+-----+-----+-----+-----+-----
     *
     * 已填充的单元格由公共测试覆盖。
     * 你可以期待空白单元格由隐藏测试覆盖！
     */

    @Test
    @Category(PublicTests.class)
    public void testCanBeParentNL() {
        // 任何锁类型都可以是 NL 的父锁
        for (LockType lockType : LockType.values()) {
            assertTrue(LockType.canBeParentLock(lockType, LockType.NL));
        }

        // 唯一可以是 NL 子锁的锁类型是 NL
        for (LockType lockType : LockType.values()) {
            if (lockType != LockType.NL) {
                assertFalse(LockType.canBeParentLock(LockType.NL, lockType));
            }
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testIXParent() {
        // IX 可以是任何类型锁的父锁
        for (LockType childType : LockType.values()) {
            assertTrue(LockType.canBeParentLock(LockType.IX, childType));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testISParent() {
        // IS 可以是 IS、S 和 NL 的父锁
        assertTrue(LockType.canBeParentLock(LockType.IS, LockType.IS));
        assertTrue(LockType.canBeParentLock(LockType.IS, LockType.S));
        assertTrue(LockType.canBeParentLock(LockType.IS, LockType.NL));

        // IS 不能是 IX、X 或 SIX 的父锁
        assertFalse(LockType.canBeParentLock(LockType.IS, LockType.IX));
        assertFalse(LockType.canBeParentLock(LockType.IS, LockType.X));
        assertFalse(LockType.canBeParentLock(LockType.IS, LockType.SIX));
    }

    /**
     * 替换矩阵
     * (左边的值是`替代锁`，顶部的值是`必需锁`)
     *
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |     |  T  |  F  |  F  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |     |  T  |  T  |  F  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |     |     |     |  T  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |     |     |     |  T  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |     |     |     |  T  |     |  T
     * ----+-----+-----+-----+-----+-----+-----
     *
     * 已填充的单元格由公共测试覆盖。
     * 你可以期待空白单元格由隐藏测试覆盖！
     *
     * 单元格中的布尔值回答以下问题：
     * "左边的锁是否可以替代顶部的锁？"
     *
     * 或者：
     * "左边锁的权限是否是顶部锁权限的超集？"
     */

    @Test
    @Category(PublicTests.class)
    public void testNLSubstitute() {
        // 除了 NL 之外，你不能用 NL 替换任何其他锁
        assertTrue(LockType.substitutable(LockType.NL, LockType.NL));
        assertFalse(LockType.substitutable(LockType.NL, LockType.S));
        assertFalse(LockType.substitutable(LockType.NL, LockType.X));
        assertFalse(LockType.substitutable(LockType.NL, LockType.IS));
        assertFalse(LockType.substitutable(LockType.NL, LockType.IX));
        assertFalse(LockType.substitutable(LockType.NL, LockType.SIX));
    }

    @Test
    @Category(PublicTests.class)
    public void testSubstitutableReal() {
        // 你不能用 IS 或 IX 替换 S
        assertFalse(LockType.substitutable(LockType.IS, LockType.S));
        assertFalse(LockType.substitutable(LockType.IX, LockType.S));

        // 你可以用 S、SIX 或 X 替换 S
        assertTrue(LockType.substitutable(LockType.S, LockType.S));
        assertTrue(LockType.substitutable(LockType.SIX, LockType.S));
        assertTrue(LockType.substitutable(LockType.X, LockType.S));

        // 你不能用 IS、IX、S 或 SIX 替换 X
        assertFalse(LockType.substitutable(LockType.IS, LockType.X));
        assertFalse(LockType.substitutable(LockType.IX, LockType.X));
        assertFalse(LockType.substitutable(LockType.S, LockType.X));
        assertFalse(LockType.substitutable(LockType.SIX, LockType.X));

        // 你可以用 X 替换 X
        assertTrue(LockType.substitutable(LockType.X, LockType.X));
    }

    @Test
    @Category(PublicTests.class)
    public void testSubstitutableIXandIS() {
        // 你可以用意图锁替换它们自己
        assertTrue(LockType.substitutable(LockType.IS, LockType.IS));
        assertTrue(LockType.substitutable(LockType.IX, LockType.IX));

        // IX 的权限是 IS 权限的超集
        assertTrue(LockType.substitutable(LockType.IX, LockType.IS));

        // IS 的权限不是 IX 权限的超集
        assertFalse(LockType.substitutable(LockType.IS, LockType.IX));
    }

}

