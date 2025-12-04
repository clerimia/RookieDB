package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * 内部特定于事务的方法，用于实现数据库的部分功能。
 *
 * 可以通过 TransactionContext::getTransaction 获取当前线程上运行的事务上下文；
 * 它只在 Transaction 调用期间设置。
 *
 * 此事务上下文实现假设每个线程一次只运行一个事务，
 * 并且除了 unblock() 方法之外，事务的方法不会从与该事务关联的不同线程调用。
 * 当调用 block() 时，此实现会阻塞线程。
 */
public abstract class TransactionContext implements AutoCloseable {
    static Map<Long, TransactionContext> threadTransactions = new ConcurrentHashMap<>();
    private boolean blocked = false;
    private boolean startBlock = false;
    private final ReentrantLock transactionLock = new ReentrantLock();
    private final Condition unblocked = transactionLock.newCondition();

    /**
     * 获取当前在此线程上运行的事务。
     * @return 当前线程上活跃运行的事务，如果没有则返回null
     */
    public static TransactionContext getTransaction() {
        long threadId = Thread.currentThread().getId();
        return threadTransactions.get(threadId);
    }

    /**
     * 设置当前在此线程上运行的事务。
     * @param transaction 当前正在运行的事务
     */
    public static void setTransaction(TransactionContext transaction) {
        long threadId = Thread.currentThread().getId();
        TransactionContext currTransaction = threadTransactions.get(threadId);
        if (currTransaction != null) {
            throw new RuntimeException("该线程已有一个正在运行的事务: " + currTransaction);
        }
        threadTransactions.put(threadId, transaction);
    }

    /**
     * 取消设置当前在此线程上运行的事务。
     */
    public static void unsetTransaction() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
            String s = trace[i].toString();
        }
        long threadId = Thread.currentThread().getId();
        TransactionContext currTransaction = threadTransactions.get(threadId);
        if (currTransaction == null) {
            throw new IllegalStateException("没有要取消设置的事务");
        }
        threadTransactions.remove(threadId);
    }

    // 状态 //////////////////////////////////////////////////////////////////

    /**
     * @return 事务编号
     */
    public abstract long getTransNum();

    /**
     * @return 分配给此事务的内存量（以页为单位）
     */
    public abstract int getWorkMemSize();

    @Override
    public abstract void close();

    // 临时表和别名 ////////////////////////////////////////////////
    /**
     * 在此事务中创建一个临时表。
     *
     * @param schema 表结构
     * @return 临时表的名称
     */
    public abstract String createTempTable(Schema schema);

    /**
     * 删除此事务中的所有临时表。
     */
    public abstract void deleteAllTempTables();

    /**
     * 为此事务指定别名映射。不允许递归别名。
     * @param aliasMap 别名到原始表名的映射
     */
    public abstract void setAliasMap(Map<String, String> aliasMap);

    /**
     * 清除所有设置的别名。
     */
    public abstract void clearAliasMap();

    // 索引 /////////////////////////////////////////////////////////////////

    /**
     * 检查数据库是否在该（表，列）上有索引。
     *
     * @param tableName  表名
     * @param columnName 列名
     * @return 如果索引存在则返回true
     */
    public abstract boolean indexExists(String tableName, String columnName);

    public abstract void updateIndexMetadata(BPlusTreeMetadata metadata);

    // 扫描 ///////////////////////////////////////////////////////////////////

    /**
     * 返回按`columnName`值升序排列的`tableName`中的记录迭代器。
     */
    public abstract Iterator<Record> sortedScan(String tableName, String columnName);

    /**
     * 返回按`columnName`值升序排列的`tableName`中的记录迭代器，
     * 仅包括该列中值大于或等于`startValue`的记录。
     */
    public abstract Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue);

    /**
     * 返回`tableName`中`columnName`值等于`key`的记录迭代器。
     */
    public abstract Iterator<Record> lookupKey(String tableName, String columnName, DataBox key);

    /**
     * 返回`tableName`中所有记录的回溯迭代器。
     */
    public abstract BacktrackingIterator<Record> getRecordIterator(String tableName);

    public abstract boolean contains(String tableName, String columnName, DataBox key);

    // 记录操作 ///////////////////////////////////////////////////////
    public abstract RecordId addRecord(String tableName, Record record);

    public abstract RecordId deleteRecord(String tableName, RecordId rid);

    public abstract void deleteRecordWhere(String tableName, String predColumnName, PredicateOperator predOperator,
                                           DataBox predValue);

    public abstract void deleteRecordWhere(String tableName, Function<Record, DataBox> cond);

    public abstract Record getRecord(String tableName, RecordId rid);

    public abstract RecordId updateRecord(String tableName, RecordId rid, Record updated);

    public abstract void updateRecordWhere(String tableName, String targetColumnName,
                                           UnaryOperator<DataBox> targetValue,
                                           String predColumnName, PredicateOperator predOperator, DataBox predValue);

    public abstract void updateRecordWhere(String tableName, String targetColumnName, Function<Record, DataBox> expr, Function<Record, DataBox> cond);

    // 表/模式 ////////////////////////////////////////////////////////////

    /**
     * @param tableName 要获取模式的表名
     * @return 表的模式
     */
    public abstract Schema getSchema(String tableName);

    /**
     * 与getSchema相同，但所有列名都是完全限定的（tableName.colName）。
     *
     * @param tableName 要获取模式的表名
     * @return 表的模式
     */
    public abstract Schema getFullyQualifiedSchema(String tableName);

    public abstract Table getTable(String tableName);

    // 统计信息 //////////////////////////////////////////////////////////////

    /**
     * @param tableName 要获取统计信息的表名
     * @return 表的TableStats对象
     */
    public abstract TableStats getStats(String tableName);

    /**
     * @param tableName 表名
     * @return 表使用的数据页数
     */
    public abstract int getNumDataPages(String tableName);

    /**
     * @param tableName 表名
     * @param columnName 列名
     * @return tableName.columnName上B+树索引的阶数
     */
    public abstract int getTreeOrder(String tableName, String columnName);

    /**
     * @param tableName 表名
     * @param columnName 列名
     * @return tableName.columnName上B+树索引的高度
     */
    public abstract int getTreeHeight(String tableName, String columnName);

    // 同步 /////////////////////////////////////////////////////////

    /**
     * prepareBlock获取事务等待的条件变量所依赖的锁。
     * 必须在block()之前调用，用于确保在事务阻塞之前不能运行与后续block()调用对应的unblock()调用。
     */
    public void prepareBlock() {
        if (this.startBlock) {
            throw new IllegalStateException("已经在准备阻塞");
        }
        this.transactionLock.lock();
        this.startBlock = true;
    }

    /**
     * 阻塞事务（和线程）。必须先调用prepareBlock()。
     */
    public void block() {
        if (!this.startBlock) {
            throw new IllegalStateException("必须在block()之前调用prepareBlock()");
        }
        try {
            this.blocked = true;
            while (this.blocked) {
                this.unblocked.awaitUninterruptibly();
            }
        } finally {
            this.startBlock = false;
            this.transactionLock.unlock();
        }
    }

    /**
     * 解除阻塞事务（和运行事务的线程）。
     */
    public void unblock() {
        this.transactionLock.lock();
        try {
            this.blocked = false;
            this.unblocked.signal();
        } finally {
            this.transactionLock.unlock();
        }
    }

    /**
     * @return 事务是否被阻塞
     */
    public boolean getBlocked() {
        return this.blocked;
    }
}
