package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Iterator;

public class SequentialScanOperator extends QueryOperator {
    private TransactionContext transaction;
    private String tableName;

    /**
     * 创建一个新的SequentialScanOperator，提供表中所有元组的迭代器。
     *
     * 注意：顺序扫描不接收源操作符，因为它们必须始终位于DAG的底部。
     *
     * @param transaction 事务上下文
     * @param tableName 表名
     */
    public SequentialScanOperator(TransactionContext transaction,
                           String tableName) {
        this(OperatorType.SEQ_SCAN, transaction, tableName);
    }

    protected SequentialScanOperator(OperatorType type,
                                     TransactionContext transaction,
                                     String tableName) {
        super(type);
        this.transaction = transaction;
        this.tableName = tableName;
        this.setOutputSchema(this.computeSchema());

        this.stats = this.estimateStats();
    }

    public String getTableName() {
        return this.tableName;
    }

    @Override
    public boolean isSequentialScan() {
        return true;
    }

    @Override
    public Iterator<Record> iterator() {
        return this.backtrackingIterator();
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        return this.transaction.getRecordIterator(tableName);
    }

    @Override
    public Schema computeSchema() {
        return this.transaction.getFullyQualifiedSchema(this.tableName);
    }

    @Override
    public String str() {
        return "Seq Scan on " + this.tableName + " (cost=" + this.estimateIOCost() + ")";
    }

    @Override
    public TableStats estimateStats() {
        return this.transaction.getStats(this.tableName);
    }

    @Override
    public int estimateIOCost() {
        return this.transaction.getNumDataPages(this.tableName);
    }

}
