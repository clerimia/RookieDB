package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.table.PageDirectory;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class QueryOperator implements Iterable<Record> {
    protected QueryOperator source;
    protected Schema outputSchema;
    protected TableStats stats;

    public enum OperatorType {
        PROJECT,
        SEQ_SCAN,
        INDEX_SCAN,
        JOIN,
        SELECT,
        GROUP_BY,
        SORT,
        LIMIT,
        MATERIALIZE
    }

    private OperatorType type;

    /**
     * 创建一个没有设置源、目标或模式的QueryOperator。
     * @param type 操作符的类型（连接、投影、选择等）
     */
    public QueryOperator(OperatorType type) {
        this.type = type;
        this.source = null;
        this.outputSchema = null;
    }

    /**
     * 创建一个设置了源的QueryOperator，并相应地计算输出模式。
     * @param type 操作符的类型（连接、投影、选择等）
     * @param source 源操作符
     */
    protected QueryOperator(OperatorType type, QueryOperator source) {
        this.source = source;
        this.type = type;
        this.outputSchema = this.computeSchema();
    }

    /**
     * @return 表示此操作符类型的枚举值（连接、投影、选择等）
     */
    public OperatorType getType() {
        return this.type;
    }

    /**
     * @return 如果此操作符是连接操作符则返回true，否则返回false。
     */
    public boolean isJoin() {
        return this.type.equals(OperatorType.JOIN);
    }

    /**
     * @return 如果此操作符是选择操作符则返回true，否则返回false。
     */
    public boolean isSelect() {
        return this.type.equals(OperatorType.SELECT);
    }

    /**
     * @return 如果此操作符是投影操作符则返回true，否则返回false。
     */
    public boolean isProject() {
        return this.type.equals(OperatorType.PROJECT);
    }

    /**
     * @return 如果此操作符是分组操作符则返回true，否则返回false。
     */
    public boolean isGroupBy() {
        return this.type.equals(OperatorType.GROUP_BY);
    }

    /**
     * @return 如果此操作符是顺序扫描操作符则返回true，否则返回false。
     */
    public boolean isSequentialScan() {
        return this.type.equals(OperatorType.SEQ_SCAN);
    }

    /**
     * @return 如果此操作符是索引扫描操作符则返回true，否则返回false。
     */
    public boolean isIndexScan() {
        return this.type.equals(OperatorType.INDEX_SCAN);
    }

    public List<String> sortedBy() {
        return Collections.emptyList();
    }

    /**
     * @return 此操作符从中获取记录的源操作符
     */
    public QueryOperator getSource() {
        return this.source;
    }

    /**
     * 设置此操作符的源并使用它来计算输出模式
     */
    protected void setSource(QueryOperator source) {
        this.source = source;
        this.outputSchema = this.computeSchema();
    }

    /**
     * @return 执行此操作符时获得的记录的模式
     */
    public Schema getSchema() {
        return this.outputSchema;
    }

    /**
     * 设置此操作符的输出模式。这应该与调用execute()获得的迭代器的记录模式匹配。
     */
    protected void setOutputSchema(Schema schema) {
        this.outputSchema = schema;
    }

    /**
     * 计算此操作符输出记录的模式。
     * @return 与调用.iterator()获得的迭代器记录模式匹配的模式
     */
    protected abstract Schema computeSchema();

    /**
     * @return 此操作符输出记录的迭代器
     */
    public abstract Iterator<Record> iterator();

    /**
     * @return 如果此查询操作符的记录物化在表中则返回true。
     */
    public boolean materialized() {
        return false;
    }

    /**
     * @throws UnsupportedOperationException 如果此操作符不支持回溯
     * @return 此操作符记录的回溯迭代器
     */
    public BacktrackingIterator<Record> backtrackingIterator() {
        throw new UnsupportedOperationException(
            "此操作符不支持回溯。您可能想要先使用QueryOperator.materialize。"
        );
    }

    /**
     * @param records 输入的记录迭代器（数据源）
     * @param schema  记录的模式
     * @param maxPages 要消耗的记录的最大页数
     * @return 此方法将从`records`中消耗最多`maxPages`页的记录（在此过程中推进它），
     * 并返回这些记录的回溯迭代器。将maxPages设置为1将导致单页记录的迭代器。
     */
    public static BacktrackingIterator<Record> getBlockIterator(Iterator<Record> records, Schema schema, int maxPages) {
        // 计算容量, 计算每页能容纳的记录数，然后计算总记录数
        int recordsPerPage = Table.computeNumRecordsPerPage(PageDirectory.EFFECTIVE_PAGE_SIZE, schema);
        int maxRecords = recordsPerPage * maxPages;

        // 提取记录，会移动迭代器指针
        List<Record> blockRecords = new ArrayList<>();
        for (int i = 0; i < maxRecords && records.hasNext(); i++) {
            blockRecords.add(records.next());
        }

        // 返回回溯的迭代器
        return new ArrayBacktrackingIterator<>(blockRecords);
    }

    /**
     * @param operator 要物化的查询操作符
     * @param transaction 物化表将在其中创建的事务
     * @return 从`operator`记录中提取的新MaterializedOperator
     */
    public static QueryOperator materialize(QueryOperator operator, TransactionContext transaction) {
        // 检查是否已经物化
        // 1.没有物化就为当前事务创建一个新的物化算子
        if (!operator.materialized()) {
            return new MaterializeOperator(operator, transaction);
        }
        return operator;
    }

    public abstract String str();

    public String toString() {
        String r = this.str();
        if (this.source != null) {
            r += ("\n-> " + this.source.toString()).replaceAll("\n", "\n\t");
        }
        return r;
    }

    /**
     * 估计执行此查询操作符的结果的表统计信息。
     *
     * @return 估计的TableStats
     */
    public abstract TableStats estimateStats();

    /**
     * 估计执行此查询操作符的IO成本。
     *
     * @return 估计的IO操作次数
     */
    public abstract int estimateIOCost();

}
