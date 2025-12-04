package edu.berkeley.cs186.database.query.disk;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.EmptyBacktrackingIterator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.List;

/**
 * Run表示磁盘上的一段空间，我们可以向其中追加记录或从中读取记录。
 * 这对于外部排序非常有用，可以在不使用记录时将其存储起来以释放内存。
 * 自动缓冲读写操作以最小化产生的I/O操作。
 */
public class Run implements Iterable<Record> {
    // 此run将用于其中的事务
    private TransactionContext transaction;
    // 在底层，我们将所有记录存储在一个临时表中
    private String tempTableName;
    private Schema schema;

    public Run(TransactionContext transaction, Schema schema) {
        this.transaction = transaction;
        this.schema = schema;
    }

    /**
     * 向此run中添加一条记录。
     * @param record 要添加的记录
     */
    public void add(Record record) {
        if (this.tempTableName == null) {
            this.tempTableName = transaction.createTempTable(schema);
        }
        this.transaction.addRecord(this.tempTableName, record);
    }

    /**
     * 向此run中添加一组记录。
     * @param records 要添加的记录列表
     */
    public void addAll(List<Record> records) {
        for (Record record: records) this.add(record);
    }

    /**
     * @return 返回此run中记录的迭代器
     */
    public BacktrackingIterator<Record> iterator() {
        if (this.tempTableName == null) return new EmptyBacktrackingIterator<>();
        return this.transaction.getRecordIterator(this.tempTableName);
    }

    /**
     * @return 返回包含此run记录的表名
     */
    public String getTableName() {
        return this.tempTableName;
    }
}
