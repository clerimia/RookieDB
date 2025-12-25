package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.common.Pair;

import java.io.ObjectStreamClass;
import java.util.Map;
import java.util.Set;

/**
 * 用于跟踪不同锁类型之间关系的工具方法。
 */
public enum LockType {
    S,   // 共享锁
    X,   // 排他锁
    IS,  // 意向共享锁
    IX,  // 意向排他锁
    SIX, // 共享意向排他锁
    NL;  // 无锁

    /**
     * 该方法检查锁类型A和B是否彼此兼容。
     * 如果一个事务可以在资源上持有锁类型A的同时，
     * 另一个事务在相同资源上持有锁类型B，则这些锁类型是兼容的。
     */
    private static final boolean[][] compatibilityMatrix = {
            // S       X      IS      IX      SIX     NL
            {true,  false,  true,   false,  false,  true}, // S
            {false, false,  false,  false,  false,  true}, // X
            {true,  false,  true,   true,   true,   true}, // IS
            {false, false,  true,   true,   false,  true}, // IX
            {false, false,  true,   false,  false,  true}, // SIX
            {true,  true,   true,   true,   true,   true}  // NL
    };

    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // 这个也就是写一个锁兼容矩阵, 然后查表返回值。
        return compatibilityMatrix[a.ordinal()][b.ordinal()];
    }

    /**
     * 该方法返回为了获得类型A的锁而应该请求的父资源上的锁。
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S: return IS;
            case X: return IX;
            case IS: return IS;
            case IX: return IX;
            case SIX: return IX;
            case NL: return NL;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * 该方法返回parentLockType是否有权限在子节点上授予childLockType锁。
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (parentLockType) {
            case S:
            case X:
            case NL:
                return childLockType.equals(NL);
            case IS:
                return childLockType.equals(NL) || childLockType.equals(S) || childLockType.equals(IS);
            case IX:
                return true;
            case SIX:
                return childLockType.equals(NL) || childLockType.equals(X) || childLockType.equals(IX);
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * 该方法返回一个锁是否可以用于需要另一种锁的情况
     * （例如，S锁可以用X锁替代，因为X锁允许事务执行S锁允许的所有操作）。
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (substitute) {
            // S 锁可以替代
            case NL:
                return required.equals(NL);
            case IS:
                return required.equals(NL) || required.equals(IS);
            case IX:
                return required.equals(IX) || required.equals(IS) || required.equals(NL);
            case S:
                return required.equals(IS) || required.equals(S) || required.equals(NL);
            case SIX:
                return required.equals(S) || required.equals(IX) || required.equals(NL) || required.equals(IS) || required.equals(SIX);
            case X:
                return true;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * @return 如果此锁是IX、IS或SIX则返回True，否则返回False。
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

