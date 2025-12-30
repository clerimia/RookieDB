package edu.berkeley.cs186.database.recovery;

public enum LogType {
    // 主日志记录（存储当前检查点）
    MASTER,
    // 分配新页面的日志记录（通过磁盘空间管理器）
    ALLOC_PAGE,
    // 更新页面部分内容的日志记录
    UPDATE_PAGE,
    // 释放页面的日志记录（通过磁盘空间管理器）
    FREE_PAGE,
    // 分配新分区的日志记录（通过磁盘空间管理器）
    ALLOC_PART,
    // 释放分区的日志记录（通过磁盘空间管理器）
    FREE_PART,
    // 开始事务提交的日志记录
    COMMIT_TRANSACTION,
    // 开始事务回滚的日志记录
    ABORT_TRANSACTION,
    // 事务完全完成后的日志记录
    END_TRANSACTION,
    // 检查点开始的日志记录
    BEGIN_CHECKPOINT,
    // 完成检查点的日志记录；一个检查点可能有多个这样的记录
    // 一个检查点
    END_CHECKPOINT,
    // 用于撤销页面分配的补偿日志记录
    UNDO_ALLOC_PAGE,
    // 用于撤销页面更新的补偿日志记录
    UNDO_UPDATE_PAGE,
    // 用于撤销页面释放的补偿日志记录
    UNDO_FREE_PAGE,
    // 用于撤销分区分配的补偿日志记录
    UNDO_ALLOC_PART,
    // 用于撤销分区释放的补偿日志记录
    UNDO_FREE_PART;

    private static LogType[] values = LogType.values();

    public int getValue() {
        return ordinal() + 1;
    }

    public static LogType fromInt(int x) {
        if (x < 1 || x > values.length) {
            String err = String.format("Unknown TypeId ordinal %d.", x);
            throw new IllegalArgumentException(err);
        }
        return values[x - 1];
    }
}
