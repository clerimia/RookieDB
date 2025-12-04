package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class IndexScanOperator extends QueryOperator {
    private TransactionContext transaction;
    private String tableName;
    private String columnName;
    private PredicateOperator predicate;
    private DataBox value;

    private int columnIndex;

    /**
     * 索引扫描操作符。
     *
     * @param transaction 包含此操作符的事务
     * @param tableName 要迭代的表
     * @param columnName 索引所在列的名称
     */
    IndexScanOperator(TransactionContext transaction,
                      String tableName,
                      String columnName,
                      PredicateOperator predicate,
                      DataBox value) {
        super(OperatorType.INDEX_SCAN);
        this.tableName = tableName;
        this.transaction = transaction;
        this.columnName = columnName;
        this.predicate = predicate;
        this.value = value;
        this.setOutputSchema(this.computeSchema());
        this.columnIndex = this.getSchema().findField(columnName);
        this.stats = this.estimateStats();
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public String str() {
        return String.format("索引扫描 %s%s%s 在表 %s 上 (成本=%d)",
            this.columnName, this.predicate.toSymbol(), this.value, this.tableName,
            this.estimateIOCost());
    }

    /**
     * 返回索引扫描所在的列名
     *
     * @return 列名
     */
    public String getColumnName() {
        return this.columnName;
    }

    @Override
    public TableStats estimateStats() {
        TableStats stats = this.transaction.getStats(this.tableName);
        return stats.copyWithPredicate(this.columnIndex,
                                       this.predicate,
                                       this.value);
    }

    @Override
    public int estimateIOCost() {
        int height = transaction.getTreeHeight(tableName, columnName);
        int order = transaction.getTreeOrder(tableName, columnName);
        TableStats tableStats = transaction.getStats(tableName);

        int count = tableStats.getHistograms().get(columnIndex).copyWithPredicate(predicate,
                    value).getCount();
        // 2 * order 个条目/叶节点，但叶节点填充度为50-100%；我们使用75%的填充因子作为粗略估计
        return (int) (height + Math.ceil(count / (1.5 * order)) + count);
    }

    @Override
    public Iterator<Record> iterator() {
        return new IndexScanIterator();
    }

    @Override
    public Schema computeSchema() {
        return this.transaction.getFullyQualifiedSchema(this.tableName);
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(this.columnName);
    }

    /**
     * 为该操作符提供迭代器接口的Iterator实现。
     */
    private class IndexScanIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private Record nextRecord;

        private IndexScanIterator() {
            this.nextRecord = null;
            if (IndexScanOperator.this.predicate == PredicateOperator.EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.lookupKey(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
            } else if (IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN ||
                       IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN_EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScan(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName);
            } else if (IndexScanOperator.this.predicate == PredicateOperator.GREATER_THAN) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
                while (this.sourceIterator.hasNext()) {
                    Record r = this.sourceIterator.next();

                    if (r.getValue(IndexScanOperator.this.columnIndex)
                            .compareTo(IndexScanOperator.this.value) > 0) {
                        this.nextRecord = r;
                        break;
                    }
                }
            } else if (IndexScanOperator.this.predicate == PredicateOperator.GREATER_THAN_EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
            }
        }

        /**
         * @return 如果此迭代器还有下一个记录要返回，则返回true，否则返回false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord != null) return true;  // 如果已有缓存记录，直接返回true
            if (!this.sourceIterator.hasNext()) return false;  // 如果底层迭代器没有更多记录，返回false
            Record r = this.sourceIterator.next();     // 获取下一条记录

            // 根据谓词类型判断记录是否符合条件
            if (predicate == PredicateOperator.LESS_THAN) {
                if (r.getValue(columnIndex).compareTo(value) < 0) {
                    this.nextRecord = r;
                }
            } else if (predicate == PredicateOperator.LESS_THAN_EQUALS) {
                if (r.getValue(columnIndex).compareTo(value) <= 0) {
                    this.nextRecord = r;
                }
            } else  {
                this.nextRecord = r;  // 对于GREATER_THAN等其他情况，构造函数已处理过滤
            }
            return this.nextRecord != null;
        }

        /**
         * @return 此迭代器的下一个记录
         * @throws NoSuchElementException 如果没有更多记录可返回
         */
        @Override
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;  // 清空缓存
                return r;
            }
            throw new NoSuchElementException();
        }
    }
}
