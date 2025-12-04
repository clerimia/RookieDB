package edu.berkeley.cs186.database.query.disk;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.SequentialScanOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.List;

/**
 * 分区表示磁盘上的一段空间，我们可以向其中追加记录或从中读取记录。
 * 这在外部哈希中很有用，可以存储我们不使用的记录并释放内存。
 * 自动缓冲读写操作以最小化产生的I/O操作。
 */
public class Partition implements Iterable<Record> {
    // 此分区将在其中使用的事务
    private TransactionContext transaction;
    // 底层我们将所有记录存储在一个临时表中
    private String tempTableName;

    public Partition(TransactionContext transaction, Schema s) {
        this.transaction = transaction;
        this.tempTableName = transaction.createTempTable(s);
    }

    /**
     * 向此分区添加一条记录。
     *
     * @param record 要添加的记录
     */
    public void add(Record record) {
        this.transaction.addRecord(this.tempTableName, record);
    }

    /**
     * 向此分区添加记录列表。
     *
     * @param records 要添加的记录
     */
    public void addAll(List<Record> records) {
        for (Record record : records) this.add(record);
    }

    /**
     * @return 返回此分区中记录的迭代器
     */
    public BacktrackingIterator<Record> iterator() {
        return this.transaction.getRecordIterator(this.tempTableName);
    }

    /**
     * @return 返回在此分区支持的临时表上的顺序扫描操作符。
     */
    public SequentialScanOperator getScanOperator() {
        return new SequentialScanOperator(this.transaction, this.tempTableName);
    }

    /**
     * 返回用于存储此分区中记录的页数。
     */
    public int getNumPages() {
        return this.transaction.getNumDataPages(this.tempTableName);
    }
}
