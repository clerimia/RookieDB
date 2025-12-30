package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.function.Consumer;

/**
 * 日志的一条记录。
 */
public abstract class LogRecord {
    // 此记录的LSN，如果没有设置则为null - 这实际上
    // 不存储在磁盘上，仅由日志管理器为了方便而设置
    protected Long LSN;
    // 此记录的类型
    protected LogType type;

    // redo()被调用时调用的方法 - 仅用于测试
    private static Consumer<LogRecord> onRedo = t -> {};

    protected LogRecord(LogType type) {
        this.type = type;
        this.LSN = null;
    }

    /**
     * @return LogType中枚举的日志条目类型
     */
    public final LogType getType() {
        return type;
    }

    /**
     * @return 日志条目的LSN
     */
    public final long getLSN() {
        if (LSN == null) {
            throw new IllegalStateException("LSN not set, has this log record been through a log manager call yet?");
        }
        return LSN;
    }

    /**
     * 设置记录的LSN
     * @param LSN 期望分配给记录的LSN
     */
    final void setLSN(Long LSN) {
        this.LSN = LSN;
    }

    /**
     * 获取日志记录的事务号（如果适用）
     * @return 包含事务号的Optional实例
     */
    public Optional<Long> getTransNum() {
        return Optional.empty();
    }

    /**
     * 获取同一事务写入的上一条记录的LSN
     * @return 包含prevLSN的Optional实例
     */
    public Optional<Long> getPrevLSN() {
        return Optional.empty();
    }

    /**
     * 获取要撤销的下一条记录的LSN（如果适用）
     * @return 包含事务号的Optional实例
     */
    public Optional<Long> getUndoNextLSN() {
        return Optional.empty();
    }

    /**
     * @return 包含页面号的Optional实例
     * 由事务更改的数据的页面号
     */
    public Optional<Long> getPageNum() {
        return Optional.empty();
    }

    /**
     * @return 包含分区号的Optional实例
     * 由事务更改的数据的分区号
     */
    public Optional<Integer> getPartNum() {
        return Optional.empty();
    }

    public Optional<Long> getMaxTransactionNum() {
        return Optional.empty();
    }

    /**
     * 获取写入日志记录的脏页表（如果适用）。
     */
    public Map<Long, Long> getDirtyPageTable() {
        return Collections.emptyMap();
    }

    /**
     * 获取写入日志记录的事务表（如果适用）。
     */
    public Map<Long, Pair<Transaction.Status, Long>> getTransactionTable() {
        return Collections.emptyMap();
    }

    /**
     * 获取事务号映射到相应事务所触及页面的页面号的表。
     */
    public Map<Long, List<Long>> getTransactionTouchedPages() {
        return Collections.emptyMap();
    }

    /**
     * @return 指示记录在日志中的事务是否可撤销的布尔值
     */
    public boolean isUndoable() {
        return false;
    }

    /**
     * @return 指示记录在日志中的事务是否可重做的布尔值
     */
    public boolean isRedoable() {
        return false;
    }

    /**
     * 返回撤销此日志记录的CLR，但不执行撤销。
     * @param lastLSN 事务的lastLSN。这将用作返回的CLR的
     *                prevLSN。
     * @return 对应于此日志记录的CLR。
     */
    public LogRecord undo(long lastLSN) {
        throw new UnsupportedOperationException("cannot undo this record: " + this);
    }

    /**
     * 执行此日志记录所描述的更改
     * @param rm 数据库的恢复管理器。
     * @param dsm 数据库的磁盘空间管理器
     * @param bm 数据库的缓冲区管理器
     */
    public void redo(RecoveryManager rm, DiskSpaceManager dsm, BufferManager bm) {
        onRedo.accept(this);
        if (!isRedoable()) {
            throw new UnsupportedOperationException("cannot redo this record: " + this);
        }
    }

    /**
     * 日志记录序列化如下：
     *
     *  - 一个1字节的整数，指示日志记录的类型，后跟
     *  - 可变数量的字节，具体取决于日志记录（详见
     *    LogRecord实现细节）。
     */
    public abstract byte[] toBytes();

    /**
     * 从缓冲区加载日志记录。
     * @param buf 包含序列化日志记录的缓冲区。
     * @return 日志记录，如果logType == 0（表示无记录的标记）则返回Optional.empty()
     * @throws UnsupportedOperationException 如果未识别日志类型
     */
    public static Optional<LogRecord> fromBytes(Buffer buf) {
        int type;
        try {
            type = buf.get();
        } catch (PageException e) {
            return Optional.empty();
        }
        if (type == 0) {
            return Optional.empty();
        }
        switch (LogType.fromInt(type)) {
        case MASTER:
            return MasterLogRecord.fromBytes(buf);
        case ALLOC_PAGE:
            return AllocPageLogRecord.fromBytes(buf);
        case UPDATE_PAGE:
            return UpdatePageLogRecord.fromBytes(buf);
        case FREE_PAGE:
            return FreePageLogRecord.fromBytes(buf);
        case ALLOC_PART:
            return AllocPartLogRecord.fromBytes(buf);
        case FREE_PART:
            return FreePartLogRecord.fromBytes(buf);
        case COMMIT_TRANSACTION:
            return CommitTransactionLogRecord.fromBytes(buf);
        case ABORT_TRANSACTION:
            return AbortTransactionLogRecord.fromBytes(buf);
        case END_TRANSACTION:
            return EndTransactionLogRecord.fromBytes(buf);
        case BEGIN_CHECKPOINT:
            return BeginCheckpointLogRecord.fromBytes(buf);
        case END_CHECKPOINT:
            return EndCheckpointLogRecord.fromBytes(buf);
        case UNDO_ALLOC_PAGE:
            return UndoAllocPageLogRecord.fromBytes(buf);
        case UNDO_UPDATE_PAGE:
            return UndoUpdatePageLogRecord.fromBytes(buf);
        case UNDO_FREE_PAGE:
            return UndoFreePageLogRecord.fromBytes(buf);
        case UNDO_ALLOC_PART:
            return UndoAllocPartLogRecord.fromBytes(buf);
        case UNDO_FREE_PART:
            return UndoFreePartLogRecord.fromBytes(buf);
        default:
            throw new UnsupportedOperationException("bad log type");
        }
    }

    /**
     * 设置在LogRecord上调用redo()时调用的方法。这只
     * 应用于测试。
     * @param handler 每次调用redo()时要调用的方法
     */
    static void onRedoHandler(Consumer<LogRecord> handler) {
        onRedo = handler;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        LogRecord logRecord = (LogRecord) o;
        return type == logRecord.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public String toString() {
        return "LogRecord{" +
               "type=" + type +
               '}';
    }
}
