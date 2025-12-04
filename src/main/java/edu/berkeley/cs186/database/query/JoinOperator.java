package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

public abstract class JoinOperator extends QueryOperator {
    public enum JoinType {
        SNLJ, // 简单嵌套循环连接
        PNLJ, // 页嵌套循环连接
        BNLJ, // 块嵌套循环连接
        SORTMERGE, // 排序合并连接
        SHJ, // 简单哈希连接 也就是需要左表能够完全载入内存
        GHJ  // 优雅哈希连接 GraceHashJoin
    }
    protected JoinType joinType; // Join方式

    // 源操作符
    private QueryOperator leftSource;
    private QueryOperator rightSource;

    // 连接列索引
    private int leftColumnIndex;
    private int rightColumnIndex;

    // 连接列名称
    private String leftColumnName;
    private String rightColumnName;

    // 当前事务
    private TransactionContext transaction;

    /**
     * 创建一个连接操作符，从leftSource和rightSource中提取元组。
     * 返回leftColumnName和rightColumnName相等的元组。
     *
     * @param leftSource 左源操作符
     * @param rightSource 右源操作符
     * @param leftColumnName 从leftSource连接的列名
     * @param rightColumnName 从rightSource连接的列名
     */
    public JoinOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction,
                 JoinType joinType) {
        super(OperatorType.JOIN);
        this.joinType = joinType;
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.leftColumnName = leftColumnName;
        this.rightColumnName = rightColumnName;
        this.setOutputSchema(this.computeSchema());
        this.transaction = transaction;
    }

    @Override
    public QueryOperator getSource() {
        throw new RuntimeException("连接操作符没有单一源。请使用" +
                                     "getRightSource和getLeftSource以及相应的set方法。");
    }

    @Override
    public Schema computeSchema() {
        // 获取记录的字段名列表
        Schema leftSchema = this.leftSource.getSchema();
        Schema rightSchema = this.rightSource.getSchema();

        // 设置连接列属性
        this.leftColumnIndex = leftSchema.findField(this.leftColumnName);
        this.rightColumnIndex = rightSchema.findField(this.rightColumnName);

        // 返回连接后的模式
        return leftSchema.concat(rightSchema);
    }

    @Override
    public String str() {
        return String.format("%s on %s=%s (cost=%d)",
                this.joinType, this.leftColumnName, this.rightColumnName,
                this.estimateIOCost());
    }

    @Override
    public String toString() {
        String r = this.str();
        if (this.leftSource != null) {
            r += ("\n-> " + this.leftSource.toString()).replaceAll("\n", "\n\t");
        }
        if (this.rightSource != null) {
            r += ("\n-> " + this.rightSource.toString()).replaceAll("\n", "\n\t");
        }
        return r;
    }

    /**
     * 估计执行此查询操作符的结果表统计信息。
     *
     * @return 估计的TableStats
     */
    @Override
    public TableStats estimateStats() {
        TableStats leftStats = this.leftSource.estimateStats();
        TableStats rightStats = this.rightSource.estimateStats();
        return leftStats.copyWithJoin(this.leftColumnIndex,
                rightStats,
                this.rightColumnIndex);
    }

    /**
     * @return 提供连接左记录的查询操作符
     */
    protected QueryOperator getLeftSource() {
        return this.leftSource;
    }

    /**
     * @return 提供连接右记录的查询操作符
     */
    protected QueryOperator getRightSource() {
        return this.rightSource;
    }

    /**
     * @return 此操作符执行所在的事务上下文
     */
    public TransactionContext getTransaction() {
        return this.transaction;
    }

    /**
     * @return 被连接的左列名称
     */
    public String getLeftColumnName() {
        return this.leftColumnName;
    }

    /**
     * @return 被连接的右列名称
     */
    public String getRightColumnName() {
        return this.rightColumnName;
    }

    /**
     * @return 左关系模式中被连接列的位置。可用于确定左关系记录中哪个值用于相等性检查。
     */
    public int getLeftColumnIndex() {
        return this.leftColumnIndex;
    }

    /**
     * @return 右关系模式中被连接列的位置。可用于确定右关系记录中哪个值用于相等性检查。
     */
    public int getRightColumnIndex() {
        return this.rightColumnIndex;
    }

    // 辅助方法 /////////////////////////////////////////////////////////////////

    /**
     * @return 如果leftRecord和rightRecord在它们的连接值上匹配则返回0，
     * 如果leftRecord的连接值小于rightRecord的连接值则返回负值，
     * 如果leftRecord的连接值大于rightRecord的连接值则返回正值。
     */
    public int compare(Record leftRecord, Record rightRecord) {
        DataBox leftRecordValue = leftRecord.getValue(this.leftColumnIndex);
        DataBox rightRecordValue = rightRecord.getValue(this.rightColumnIndex);
        return leftRecordValue.compareTo(rightRecordValue);
    }
}
