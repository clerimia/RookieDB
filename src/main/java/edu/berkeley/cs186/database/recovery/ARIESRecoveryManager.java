package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * ARIES 实现。
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // 磁盘空间管理器。
    DiskSpaceManager diskSpaceManager;
    // 缓冲区管理器。
    BufferManager bufferManager;

    // 使用给定事务号创建新事务用于恢复的函数。 传入一个事务号，创建一个事务
    private Function<Long, Transaction> newTransaction;

    // 日志管理器
    LogManager logManager;
    // 脏页表（页号 -> recLSN）。
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // 事务表（事务号 -> 条目）。
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // 如果重启的重做阶段已终止则为 true，否则为 false。用于
    // 防止在 restartRedo 期间刷新 DPT 条目。
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * 初始化日志；仅在第一次设置数据库时调用。
     * 应将主记录添加到日志中，并且应该
     * 进行检查点。
     */
    @Override
    public void initialize() {
        // 写入一个主记录
        this.logManager.appendToLog(new MasterLogRecord(0));
        // 执行检查点
        this.checkpoint();
    }

    /**
     * 设置缓冲区/磁盘管理器。这不是构造函数的一部分
     * 因为缓冲管理器和恢复管理器之间存在循环依赖
     * （缓冲管理器必须与恢复管理器接口以阻止页面驱逐直到日志被刷新，但恢复管理器需要与缓冲管理器接口以写入日志和重做更改）。
     * @param diskSpaceManager 磁盘空间管理器
     * @param bufferManager 缓冲区管理器
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // 前向处理 //////////////////////////////////////////////////////

    /**
     * 当一个新事务开始时调用。
     *
     * 事务应该被添加到事务表中。
     *
     * @param transaction 新事务
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
        // 初始的时候 <transactionNum, lastLSN = 0, transaction.status = Running>
    }

    /**
     * 当一个事务即将开始提交时调用。
     *
     * 应该追加提交记录，刷新日志，
     * 并更新事务表和事务状态。
     *
     * @param transNum 正在提交的事务
     * @return Commit Log Record的LSN
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // 1. 先生成一个Commit Log Record 写入日志缓冲区
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        long LSN = logManager.appendToLog(new CommitTransactionLogRecord(transNum, transactionTableEntry.lastLSN));

        // 2. 刷入日志
        logManager.flushToLSN(LSN);

        // 3. 更新事务表，和事务状态
        transactionTableEntry.lastLSN = LSN;
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        return LSN;
    }

    /**
     * 当一个事务被设置为中止时调用。
     *
     * 应该追加中止记录，并更新事务表和事务状态。调用此函数不应执行任何回滚。
     *
     * @param transNum 正在中止的事务
     * @return 中止记录的LSN
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        // 1. 写入一个回滚日志到日志缓冲区中
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        long LSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, transactionTableEntry.lastLSN));

        // 2. 更新事务状态和事务表
        transactionTableEntry.transaction.setStatus(Transaction.Status.ABORTING);
        transactionTableEntry.lastLSN = LSN;
        return LSN;
    }

    /**
     * 当一个事务正在清理时调用；如果事务正在中止，这应该回滚更改（参见下面的rollbackToLSN辅助函数）。
     *
     * 需要撤销的任何更改都应该被撤销，事务应该从事务表中移除，应追加结束记录，并应更新事务状态。
     *
     * @param transNum 要结束的事务
     * @return 结束记录的LSN
     */
    @Override
    public long end(long transNum) {
        // TODO
        // 获取 TransactionEntry
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // 如果事务的状态是 ABORTING，需要执行回滚
        if (transactionTableEntry.transaction.getStatus() == Transaction.Status.ABORTING) {
            rollbackToLSN(transNum, 0);
        }

        // 将事务状态切换为 COMPLETE
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        
        // 写入一个 End 记录
        long endLSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, transactionTableEntry.lastLSN));
        transactionTableEntry.lastLSN = endLSN;
        
        // 将事务从事务表中移除
        transactionTable.remove(transNum);

        // 不需要刷盘，因为恢复中的分析阶段可以重建END记录
        return endLSN;
    }

    /**
     * 推荐的辅助函数：执行一个事务的所有操作的回滚，直到（但不包括）某个LSN。
     * 从最近的尚未被撤销的记录的LSN开始：
     * - 当前LSN大于我们要回滚到的LSN时：
     *    - 如果当前LSN处的记录是可撤销的：
     *       - 通过调用记录上的undo获取补偿日志记录（CLR）
     *       - 追加CLR
     *       - 调用CLR上的redo来执行撤销
     *    - 将当前LSN更新为下一个要撤销的记录的LSN
     *
     * 注意上面调用记录上的.undo()不会执行撤销，它只是创建补偿日志记录。
     *
     * @param transNum 要执行回滚的事务
     * @param LSN 我们应该回滚到的LSN
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // 小优化：如果最后的记录是CLR，我们可以从下一个尚未被撤销的记录开始回滚。
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);

        // TODO(proj5) 实现上面描述的回滚逻辑
        while (currentLSN > LSN) {
            // 获取当前的记录
            LogRecord currentLog = logManager.fetchLogRecord(currentLSN);
            // 1. 判断该记录是否可以 undo
            if (currentLog.isUndoable()) {
                // 1.1 如果可以回滚，就调用该记录的 Undo 生成一个 CLR 日志
                LogRecord CLRLog = currentLog.undo(lastRecordLSN);
                // 1.2 将这个CLR日志追加到日志缓冲区
                lastRecordLSN = logManager.appendToLog(CLRLog);
                // 1.3 更新事务表
                transactionEntry.lastLSN = lastRecordLSN;
                // 1.4 redo CLR 做真正的回滚
                CLRLog.redo(this, diskSpaceManager, bufferManager);
            }
            // 2. 更新currentLSN为下一个要撤销的记录
            // 对于可撤销记录：使用其undoNextLSN（如果存在，通常不存在）或prevLSN
            // 对于CLR记录：使用其undoNextLSN跳过已撤销的记录
            // 对于其他不可撤销记录（如Abort）：使用prevLSN
            currentLSN = currentLog.getUndoNextLSN().orElse(currentLog.getPrevLSN().orElse(0L));
        }
    }

    /**
     * 在页面从缓冲区缓存刷新之前调用。
     * 此方法永远不会在日志页面上调用。
     *
     * 日志应刷新到必要的位置。
     *
     * @param pageLSN 即将刷新的页面的pageLSN
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * 当页面已在磁盘上更新时调用。
     *
     * 由于页面不再脏，应该从脏页表中移除。
     *
     * @param pageNum 在磁盘上更新的页面的页号
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * 当页面写入发生时调用。
     *
     * 此方法永远不会在日志页面上调用。before和after参数的参数保证长度相同。
     *
     * 应追加适当的日志记录，事务表和脏页表应相应更新。
     *
     * @param transNum 执行写入的事务
     * @param pageNum 正在写入的页面的页号
     * @param pageOffset 页面中写入开始的偏移量
     * @param before 写入前从pageOffset开始的字节
     * @param after 写入后从pageOffset开始的字节
     * @return 写入日志的最后记录的LSN
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        // 获取事务表项
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        // 1. 先创建对应的页面更新日志记录
        UpdatePageLogRecord updatePageLogRecord = new UpdatePageLogRecord(transNum, pageNum, transactionTableEntry.lastLSN, pageOffset, before, after);
        // 2. 写入日志
        long LSN = logManager.appendToLog(updatePageLogRecord);
        // 3. 更新事务表
        transactionTableEntry.lastLSN = LSN;
        // 4. 进行页面修改
        dirtyPage(pageNum, LSN);
        // 返回LSN
        return LSN;
    }


    /**
     * 当分配新分区时调用。需要日志刷新，
     * 因为更改在此返回后立即在磁盘上可见。
     *
     * 如果分区是日志分区，此方法应返回-1。
     *
     * 应追加适当的日志记录，并刷新日志。
     * 事务表应相应更新。
     *
     * @param transNum 请求分配的事务
     * @param partNum 新分区的分区号
     * @return 记录的LSN或-1（如果为日志分区）
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // 如果是日志的一部分则忽略。
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // 更新lastLSN
        transactionEntry.lastLSN = LSN;
        // 刷新日志
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 当分区被释放时调用。需要日志刷新，
     * 因为更改在此返回后立即在磁盘上可见。
     *
     * 如果分区是日志分区，此方法应返回-1。
     *
     * 应追加适当的日志记录，并刷新日志。
     * 事务表应相应更新。
     *
     * @param transNum 请求释放分区的事务
     * @param partNum 要释放的分区的分区号
     * @return 记录的LSN或-1（如果为日志分区）
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // 如果是日志的一部分则忽略。
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // 更新lastLSN
        transactionEntry.lastLSN = LSN;
        // 刷新日志
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 当分配新页面时调用。需要日志刷新，
     * 因为更改在此返回后立即在磁盘上可见。
     *
     * 如果页面在日志分区中，此方法应返回-1。
     *
     * 应追加适当的日志记录，并刷新日志。
     * 事务表应相应更新。
     *
     * @param transNum 请求分配的事务
     * @param pageNum 新页面的页号
     * @return 记录的LSN或-1（如果为日志分区）
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // 如果是日志的一部分则忽略。
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // 更新lastLSN
        transactionEntry.lastLSN = LSN;
        // 刷新日志
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 当页面被释放时调用。需要日志刷新，
     * 因为更改在此返回后立即在磁盘上可见。
     *
     * 如果页面在日志分区中，此方法应返回-1。
     *
     * 应追加适当的日志记录，并刷新日志。
     * 事务表应相应更新。
     *
     * @param transNum 请求释放页面的事务
     * @param pageNum 要释放的页面的页号
     * @return 记录的LSN或-1（如果为日志分区）
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // 如果是日志的一部分则忽略。
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // 更新lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // 刷新日志
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 为事务创建保存点。使用与现有保存点
     * 相同的名称为事务创建保存点应
     * 删除旧的保存点。
     *
     * 应记录适当的LSN，以便稍后
     * 可以进行部分回滚。
     *
     * @param transNum 要创建保存点的事务
     * @param name 保存点名称
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * 释放（删除）事务的保存点。
     * @param transNum 要删除保存点的事务
     * @param name 保存点名称
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * 将事务回滚到保存点。
     *
     * 自保存点以来由事务完成的所有更改都应该被撤销，按相反顺序，将适当的CLR写入日志。事务状态应保持不变。
     *
     * @param transNum 要部分回滚的事务
     * @param name 保存点名称
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // 应撤销LSN处记录之后的所有事务更改。
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
        return;
    }

    /**
     * 创建检查点。
     *
     * 首先，应写入开始检查点记录。
     *
     * 然后，应尽可能多地使用来自DPT的recLSN填充结束检查点记录，
     * 然后是事务表中的状态/lastLSN，并在满时（或没有剩余要写入时）写入。
     * 您可以在此处找到方法EndCheckpointLogRecord#fitsInOneRecord
     * 以确定何时写入结束检查点记录。
     *
     * 最后，主记录应使用
     * 开始检查点记录的LSN重写。
     */
    @Override
    public synchronized void checkpoint() {
        // 创建开始检查点日志记录并写入日志
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();


        // TODO(proj5): 为DPT和事务表生成结束检查点记录
        // 判断当前的所有表能否被一个记录容纳
        // 获取两个表的迭代器
        Iterator<Map.Entry<Long, Long>> dptIterator = dirtyPageTable.entrySet().iterator();
        Iterator<Map.Entry<Long, TransactionTableEntry>> txnTableIterator = transactionTable.entrySet().iterator();
        while (dptIterator.hasNext()) {
            // 判断复制当前记录是否会导致检查点记录过大，如果是，就先生成一条记录，然后清空缓存
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, chkptTxnTable.size())) {
                LogRecord record = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(record);
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            // 获取当前项
            Map.Entry<Long, Long> entry = dptIterator.next();
            chkptDPT.put(entry.getKey(), entry.getValue());
        }
        while (txnTableIterator.hasNext()) {
            // 判断复制当前记录是否会导致检查点记录过大，如果是，就先生成一条记录，然后清空缓存
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                LogRecord record = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(record);
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            Map.Entry<Long, TransactionTableEntry> entry = txnTableIterator.next();
            chkptTxnTable.put(entry.getKey(), new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
        }

        // 最后的结束检查点记录，可以是空的
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);

        // 确保检查点完全刷新后才更新主记录
        flushToLSN(endRecord.getLSN());

        // 更新主记录
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * 将日志刷新到至少指定的记录，
     * 本质上刷新到包含LSN指定记录的页面。
     *
     * @param LSN 日志应刷新到的LSN
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // 处理早期日志被后期日志
        // 插入竞争的竞态条件。
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // 重启恢复 ////////////////////////////////////////////////////////

    /**
     * 每当数据库启动时调用，并执行重启恢复。
     * 当返回的Runnable运行到终止时，恢复完成。
     * 一旦此方法返回，就可以开始新事务。
     *
     * 这应该执行恢复的三个阶段，还应在重做和撤销之间
     * 清理非脏页（在
     * 缓冲管理器中不是脏页）的脏页表，并在
     * 撤销后执行检查点。
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * 此方法执行重启恢复的分析阶段。
     *
     * 首先，应读取主记录（LSN 0）。主记录包含一条信息：上次成功检查点的LSN。
     *
     * 然后我们开始扫描日志记录，从上次成功检查点的开始处开始。
     *
     * 如果日志记录是事务操作（存在getTransNum）
     * - 更新事务表
     *
     * 如果日志记录与页面相关（存在getPageNum），更新dpt
     *   - 更新/撤销更新页面将使页面变脏
     *   - 释放/撤销分配页面总是将更改刷新到磁盘
     *   - 对于分配/撤销释放页面不需要操作
     *
     * 如果日志记录是事务状态更改：
     * - 更新事务状态为COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - 更新事务表
     * - 如果END_TRANSACTION：清理事务（Transaction#cleanup），从
     *   事务表中移除，并添加到endedTransactions
     *
     * 如果日志记录是end_checkpoint记录：
     * - 复制检查点DPT的所有条目（如果存在则替换现有条目）
     * - 跳过已完成事务的事务表条目
     * - 如果尚未存在则添加到事务表
     * - 将lastLSN更新为现有条目（如果有）和检查点的较大者
     * - 如果可以从表中的状态转换到
     *   检查点中的状态，则应更新事务表中的状态。例如，running -> aborting 是可能的转换，但aborting -> running 不是。
     *
     * 处理完日志中的所有记录后，对于每个Ttable条目：
     *  - 如果COMMITTING：清理事务，将状态更改为COMPLETE，
     *    从ttable中移除，并追加结束记录
     *  - 如果RUNNING：将状态更改为RECOVERY_ABORTING，并追加中止
     *    记录
     *  - 如果RECOVERY_ABORTING：不需要操作
     */
    void restartAnalysis() {
        // 读取主记录
        LogRecord record = logManager.fetchLogRecord(0L);
        // 类型检查
        assert (record != null && record.getType() == LogType.MASTER);
        // TODO 暴力修改
        if (record == null || record.getType() != LogType.MASTER) {
            return;
        }
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // 获取开始检查点LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // 已完成事务的集合
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        // 获取日志记录迭代器
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(LSN);
        LogRecord currentLogRecord = null;
        // 遍历它
        while (logRecordIterator.hasNext()) {
            currentLogRecord = logRecordIterator.next();
            LogType logType = currentLogRecord.type;


            // 1. 如果是BeginCheckPoint，什么都不做
            if (logType == LogType.BEGIN_CHECKPOINT) {
                continue;
            }
            // 如果是 END_CHECKPOINT 记录
            if (logType == LogType.END_CHECKPOINT) {
                // 获取记录中保存的表
                Map<Long, Long> checkpointDPT = currentLogRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> recordTransactionTable = currentLogRecord.getTransactionTable();
                // 始终使用检查点的 recLSN
                dirtyPageTable.putAll(checkpointDPT);
                // 遍历事务表
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> logTransactionEntry : recordTransactionTable.entrySet()) {
                    Long transNum = logTransactionEntry.getKey();
                    // 1. 如果在 endedTransactions 中，则跳过
                    if (endedTransactions.contains(transNum)) continue;
                    // 2. 如果事务表中没有，就重建
                    if (!transactionTable.containsKey(transNum)) {
                        startTransaction(newTransaction.apply(transNum));
                    }
                    // 3. 如果事务表中有了，就更新lastLSN
                    TransactionTableEntry transactionEntry = transactionTable.get(transNum);
                    Long logLastLSN = logTransactionEntry.getValue().getSecond();
                    if (logLastLSN >= transactionEntry.lastLSN) {
                        transactionEntry.lastLSN = logLastLSN;
                    }
                    // 4. 更新事务状态
                    // 由于检查点中的事务状态仅有可能是：COMMITTING、ABORTING、RUNNING
                    // 内存事务表中的状态仅有可能是：COMMITTING、RUNNING
                    if (transactionEntry.transaction.getStatus() == Transaction.Status.RUNNING && logTransactionEntry.getValue().getFirst() == Transaction.Status.ABORTING) {
                        transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    }
                    if (transactionEntry.transaction.getStatus() == Transaction.Status.RUNNING && logTransactionEntry.getValue().getFirst() == Transaction.Status.COMMITTING) {
                        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
                    }
                }
                continue;
            }
            // 2. 如果是事务操作记录，创建事务，或者更新事务的lastLSN
            Long transNum = currentLogRecord.getTransNum().orElse(-1L);
            if (transNum != -1L) {
                if (!transactionTable.containsKey(transNum)) {
                    // 如果内存事务表不存在对应的事务，那就创建
                    startTransaction(newTransaction.apply(transNum));
                }
                // 更新事务的 lastLSN
                TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
                transactionTableEntry.lastLSN = currentLogRecord.getLSN();

                // 3. 如果是页面操作记录，可能要更新DPT
                if (logType == LogType.UPDATE_PAGE || logType == LogType.UNDO_UPDATE_PAGE) {
                    dirtyPage(currentLogRecord.getPageNum().get(), currentLogRecord.getLSN());
                }
                // 4. 如果是FreePage，UndoAllocPage，就是释放页面
                if (logType == LogType.FREE_PAGE || logType == LogType.UNDO_ALLOC_PAGE) {
                    dirtyPageTable.remove(currentLogRecord.getPageNum().get());
                }
                // 到这里事务肯定在
                // 5. 如果是事务状态操作，更新事务状态
                if (logType == LogType.COMMIT_TRANSACTION) {
                    transactionTableEntry.transaction.setStatus(Transaction.Status.COMMITTING);
                }
                if (logType == LogType.ABORT_TRANSACTION) {
                    transactionTableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                }
                if (logType == LogType.END_TRANSACTION) {
                    // 如果发现这个事务已经结束，那么就结束它
                    transactionTableEntry.transaction.cleanup(); // 不会调用 end(transNum)
                    // 设置事务状态
                    transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                    // 将事务从事务表中移除
                    transactionTable.remove(transNum);
                    // 将事务添加到结束事务的集合中
                    endedTransactions.add(transNum);
                }
            }


        }

        // 到这里就遍历完成了，需要遍历事务表，然后进行分析阶段的收尾工作
        // 收尾结束后，事务表只剩下一种状态的事务了: RECOVERY_ABORTING
        Iterator<Map.Entry<Long, TransactionTableEntry>> iterator = transactionTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, TransactionTableEntry> entry = iterator.next();
            Long transNum = entry.getKey();
            TransactionTableEntry transEntry = entry.getValue();
            // 这里的事务状态只有三种可能：Running、RECOVERY_ABORTING、COMMITTING
            if (transEntry.transaction.getStatus() == Transaction.Status.RUNNING) {
                // 1. 如果是RUNNING 就需要写入一条 ABORT 记录，然后将事务状态设置为RECOVERY_ABORTING
                transEntry.lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, transEntry.lastLSN));
                transEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
            if (transEntry.transaction.getStatus() == Transaction.Status.COMMITTING) {
                // 2. 如果是COMMITTING 就需要将它结束，因为日志已经持久化了
                // 2.1 写入一条END日志
                transEntry.lastLSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, transEntry.lastLSN));
                // 2.2 清理事务
                transEntry.transaction.cleanup();
                // 2.3 更新事务状态
                transEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                // 2.4 将事务加入 endedTransactions
                endedTransactions.add(transNum);
                // 2.5 移除事务项
                iterator.remove();
            }
        }
    }

    /**
     * 此方法执行重启恢复的重做阶段。
     *
     * 首先，从脏页表确定REDO的起始点。
     *
     * 然后，从起始点开始扫描，如果记录是可重做的且
     * - 与分区相关（Alloc/Free/UndoAlloc/UndoFree..Part），总是重做它
     * - 分配页面（AllocPage/UndoFreePage），总是重做它
     * - 修改脏页表中的页面（Update/UndoUpdate/Free/UndoAlloc....Page）
     *   且LSN >= recLSN，从磁盘获取页面，检查pageLSN，如有需要重做记录。
     */
    void restartRedo() {
        // TODO(proj5): implement
        // 1. 通过 分析阶段重建的DPT脏页表分析 REDO 阶段的起点： min(recLSN)
        Long beginLSN = Long.MAX_VALUE;
        for (Long pageNum : dirtyPageTable.keySet()) {
            Long recLSN = dirtyPageTable.get(pageNum);
            if (recLSN < beginLSN) {
                beginLSN = recLSN;
            }
        }
        if (beginLSN == Long.MAX_VALUE) {
            return;
        }

        // 2. 获取从beginLSN 开始的日志迭代器
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(beginLSN);

        // 3. 开始迭代
        LogRecord curLogRecord = null;
        LogType logType = null;
        while (logRecordIterator.hasNext()) {
            curLogRecord = logRecordIterator.next();
            logType = curLogRecord.type;
            // 3.1 如果记录不可重做就跳过
            if (!curLogRecord.isRedoable()) continue;
            // 3.2 如果是页面操作记录: (写页面，刷页面)
            if (logType == LogType.UPDATE_PAGE || logType == LogType.UNDO_UPDATE_PAGE || logType == LogType.FREE_PAGE || logType == LogType.UNDO_ALLOC_PAGE) {
                // 3.2.1 如果页面不在 DPT 中，跳过，因为页面已经持久化了
                Long pageNum = curLogRecord.getPageNum().get();
                long curLSN = curLogRecord.getLSN();
                if (!dirtyPageTable.containsKey(pageNum)) continue;
                // 这里就可以获取 dptEntry了
                Long recLSN = dirtyPageTable.get(pageNum);
                // 3.2.2 页面在DPT 中， 需要检查记录的 LSN 是否小于 recLSN，如果小于代表该记录已经被持久化了
                if (curLSN < recLSN) continue;
                // 3.2.3 页面在DPT中，LSN >= recLSN，就要查看页面中的pageLSN，如果LSN <= pageLSN 就代表记录已经持久化了
                Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                try {
                    // Do anything that requires the page here
                    if (curLSN <= page.getPageLSN()) continue;
                } finally {
                    page.unpin();
                }
            }
            curLogRecord.redo(this, diskSpaceManager, bufferManager);
        }
    }

    /**
     * 此方法执行重启恢复的撤销阶段。

     * 首先，创建一个按所有中止事务的lastLSN排序的优先队列。
     *
     * 然后，始终在优先队列中处理最大的LSN直到完成，
     * - 如果记录是可撤销的，撤销它，并追加适当的CLR
     * - 使用undoNextLSN（如果可用）替换条目，
     *   否则使用prevLSN。
     * - 如果新LSN为0，清理事务，将状态设置为complete，
     *   并从事务表中移除。
     */
    void restartUndo() {
        // TODO(proj5): implement
        // 1. 创建 toUndo 优先队列
        PriorityQueue<Pair<Long, Long>> toUndo = new PriorityQueue<>(new PairFirstReverseComparator<>());

        // 2. 初始化 toUndo <lastLSN, transNum>
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            toUndo.offer(new Pair<>(entry.getValue().lastLSN, entry.getKey()));
        }

        // 3. 开始遍历
        LogRecord curLogRecord = null;
        while (!toUndo.isEmpty()) {
            Pair<Long, Long> pair = toUndo.poll();
            Long LSN = pair.getFirst();
            Long transNum = pair.getSecond();
            TransactionTableEntry transEntry = transactionTable.get(transNum);
            curLogRecord = logManager.fetchLogRecord(LSN);
            // 4. 如果可以 undo
            if (curLogRecord.isUndoable()) {
                // 4.1 生成一个对应的 CLR 记录
                LogRecord CLR = curLogRecord.undo(transEntry.lastLSN);
                transEntry.lastLSN = logManager.appendToLog(CLR);
                // 4.2 进行撤销
                CLR.redo(this, diskSpaceManager, bufferManager);
            }
            // 5. 跟随这个记录链
            Long nextLSN = curLogRecord.getUndoNextLSN().orElse(curLogRecord.getPrevLSN().orElse(0L));
            if (nextLSN == 0L) {
                // 6. 如果这个记录就是最后一条了，那么当前事务就结束了
                // 6.1 写入一条END日志
                transEntry.lastLSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, transEntry.lastLSN));
                // 6.2 清理事务
                transEntry.transaction.cleanup();
                // 6.3 更新事务状态
                transEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                // 6.4 将事务从事务表中移除
                transactionTable.remove(transNum);
            } else {
                // 7. 加入这个
                toUndo.offer(new Pair<>(nextLSN, transNum));
            }
        }
    }

    /**
     * 从DPT中移除缓冲管理器中不是脏页的页面。
     * 这很慢，只应在恢复期间使用。
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // 辅助方法 /////////////////////////////////////////////////////////////////
    /**
     * 仅比较Pair<A, B>的第一个元素（类型A）的比较器，
     * 按相反顺序。
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
