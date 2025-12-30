package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;

/**
 * 恢复管理器的接口。
 */
public interface RecoveryManager extends AutoCloseable {
    /**
     * 初始化日志；仅在第一次设置数据库时调用。
     */
    void initialize();

    /**
     * 设置缓冲区/磁盘管理器。这不在构造函数中，因为缓冲区管理器和恢复管理器之间存在循环依赖
     * （缓冲区管理器必须与恢复管理器交互以阻止页面驱逐直到日志被刷新，但恢复管理器需要与缓冲区管理器交互以写入日志和重做更改）。
     * @param diskSpaceManager 磁盘空间管理器
     * @param bufferManager 缓冲区管理器
     */
    void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager);

    /**
     * 启动新事务时调用。
     * @param transaction 新事务
     */
    void startTransaction(Transaction transaction);

    /**
     * 事务即将开始提交时调用。
     * @param transNum 正在提交的事务
     * @return 提交记录的LSN
     */
    long commit(long transNum);

    /**
     * 事务被设置为中止时调用。
     * @param transNum 正在中止的事务
     * @return 中止记录的LSN
     */
    long abort(long transNum);

    /**
     * 在事务清理时调用；如果事务正在中止，应该回滚更改。
     * @param transNum 要结束的事务
     * @return 结束记录的LSN
     */
    long end(long transNum);

    /**
     * 在页面从缓冲区缓存刷新之前调用。此方法永远不会在日志页面上调用。
     *
     * @param pageLSN 即将被刷新的页面的LSN
     */
    void pageFlushHook(long pageLSN);

    /**
     * 页面在磁盘上更新时调用。
     * @param pageNum 在磁盘上更新的页面的页号
     */
    void diskIOHook(long pageNum);

    /**
     * 页面写入发生时调用。
     * <br/>
     * 此方法永远不会在日志页面上调用。before 和 after 参数的长度必须相同。
     *
     * @param transNum 执行写入的事务
     * @param pageNum 正在写入的页面的页号
     * @param pageOffset 写入开始的页面偏移量
     * @param before 写入前从pageOffset开始的字节
     * @param after 写入后从pageOffset开始的字节
     * @return 写入日志的最后一条记录的LSN
    */
    long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                      byte[] after);

    /**
     * 分配新分区时调用。需要刷新日志，因为更改在此返回后立即在磁盘上可见。
     * <br/>
     * 如果分区是日志分区，此方法应返回-1。
     *
     * @param transNum 请求分配的事务
     * @param partNum 新分区的分区号
     * @return 记录的LSN或-1（如果是日志分区）
     */
    long logAllocPart(long transNum, int partNum);

    /**
     * 释放分区时调用。需要刷新日志，因为更改在此返回后立即在磁盘上可见。
     * <br/>
     * 如果分区是日志分区，此方法应返回-1。
     *
     * @param transNum 请求释放分区的事务
     * @param partNum 被释放分区的分区号
     * @return 记录的LSN 或 -1（如果是日志分区）
     */
    long logFreePart(long transNum, int partNum);

    /**
     * 分配新页面时调用。需要刷新日志，因为更改在此返回后立即在磁盘上可见。
     * <br/>
     * 如果页面在日志分区中，此方法应返回-1。
     *
     * @param transNum 请求分配的事务
     * @param pageNum 新页面的页号
     * @return 记录的LSN或-1（如果是日志分区）
     */
    long logAllocPage(long transNum, long pageNum);

    /**
     * 释放页面时调用。需要刷新日志，因为更改在此返回后立即在磁盘上可见。
     * <br/>
     * 如果页面在日志分区中，此方法应返回-1。
     *
     * @param transNum 请求释放页面的事务
     * @param pageNum 被释放页面的页号
     * @return 记录的LSN或-1（如果是日志分区）
     */
    long logFreePage(long transNum, long pageNum);

    /**
     * 为事务创建保存点。使用与现有保存点相同名称创建保存点应删除旧保存点。
     * @param transNum 为其创建保存点的事务
     * @param name 保存点的名称
     */
    void savepoint(long transNum, String name);

    /**
     * 释放（删除）事务的保存点。
     * @param transNum 为其删除保存点的事务
     * @param name 保存点的名称
     */
    void releaseSavepoint(long transNum, String name);

    /**
     * 将事务回滚到保存点。
     * @param transNum 部分回滚的事务
     * @param name 保存点的名称
     */
    void rollbackToSavepoint(long transNum, String name);

    /**
     * 创建检查点。
     */
    void checkpoint();

    /**
     * 将日志刷新到指定记录，
     * 实质上是刷新到包含LSN指定记录的页面（包括该页面）。
     *
     * @param LSN 日志应刷新到的LSN
     */
    void flushToLSN(long LSN);

    /**
     * 如果页面尚未存在，则将给定页面号和LSN添加到脏页表中。
     * @param pageNum 页面号
     * @param LSN LSN
     */
    void dirtyPage(long pageNum, long LSN);

    /**
     * 每当数据库启动时调用，并执行重启恢复。
     * 此方法返回后可以开始新事务。
     * 也就是这是一个同步方法
     */
    void restart();

    /**
     * 清理：日志刷新、检查点等。在数据库关闭时调用。
     * 保存当前数据库状态，持久化
     */
    @Override
    void close();
}
