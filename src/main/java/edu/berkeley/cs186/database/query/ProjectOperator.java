package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class ProjectOperator extends QueryOperator {
    // 用于在此运算符输出中使用的列名列表
    private List<String> outputColumns;

    // 此查询的GROUP BY子句中的列名
    private List<String> groupByColumns;

    // 源运算符的模式
    private Schema sourceSchema;

    // 将为每条记录计算的表达式列表。每个表达式对应于outputColumns中的一列。
    private List<Expression> expressions;

    /**
     * 创建一个新的ProjectOperator，从源读取元组并过滤掉列。
     * 如果指定了聚合，则可选择计算聚合。
     *
     * @param source
     * @param columns
     * @param groupByColumns
     */
    public ProjectOperator(QueryOperator source, List<String> columns, List<String> groupByColumns) {
        super(OperatorType.PROJECT);
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            // 没有提供表达式对象，手动解析每个输入列
            expressions.add(Expression.fromString(columns.get(i)));
        }
        initialize(source, columns, expressions, groupByColumns);
    }

    // 直接提供了表达式对象
    public ProjectOperator(QueryOperator source, List<String> columns, List<Expression> expressions, List<String> groupByColumns) {
        super(OperatorType.PROJECT);
        initialize(source, columns, expressions, groupByColumns);
    }

    public void initialize(QueryOperator source, List<String> columns, List<Expression> expressions, List<String> groupByColumns) {
        this.outputColumns = columns;
        this.groupByColumns = groupByColumns;
        this.expressions = expressions;
        this.sourceSchema = source.getSchema();
        this.source = source;

        // 计算Schema
        Schema schema = new Schema();
        for (int i = 0; i < columns.size(); i++) {
            expressions.get(i).setSchema(this.sourceSchema);
            // 添加字段 列名就是输入的列名   可能直接是表达式字符串
            schema.add(columns.get(i), expressions.get(i).getType());
        }
        this.outputSchema = schema;

        // 确定哪些字段是GROUP BY字段
        Set<Integer> groupByIndices = new HashSet<>();
        for (String colName: groupByColumns) {
            groupByIndices.add(this.sourceSchema.findField(colName));
        }
        // 看是否有聚合函数
        boolean hasAgg = false;
        for (int i = 0; i < expressions.size(); i++) {
            hasAgg |= expressions.get(i).hasAgg();
        }
        if (!hasAgg) return; // 没有就不进行下一步了


        /**
        *   这是最重要的验证部分，实现了 SQL 中 GROUP BY 的语义规则：
        *   对于每个表达式，获取它所依赖的列集合
        *   对于非聚合表达式（例如普通的列），检查它依赖的列是否都在 GROUP BY 列中
        *   如果有不属于 GROUP BY 的列被引用，则抛出异常，这符合 SQL 标准中"SELECT 子句中的非聚合列必须出现在 GROUP BY 子句中"的规则
        * */
        for (int i = 0; i < expressions.size(); i++) {
            Set<Integer> dependencyIndices = new HashSet<>();
            for (String colName: expressions.get(i).getDependencies()) {
                dependencyIndices.add(this.sourceSchema.findField(colName));
            }
            if (!expressions.get(i).hasAgg()) {
                dependencyIndices.removeAll(groupByIndices);
                if (dependencyIndices.size() != 0) {
                    int any = dependencyIndices.iterator().next();

                    // 有不属于 GROUP BY 的列进行了聚合运算
                    throw new UnsupportedOperationException(
                            "非聚合表达式 `" + columns.get(i) +
                                    "` 引用了未分组字段 `" + sourceSchema.getFieldName(any) + "`"
                    );
                }
            }
        }
    }

    @Override
    public boolean isProject() { return true; }

    @Override
    protected Schema computeSchema() {
        return this.outputSchema;
    }

    @Override
    public Iterator<Record> iterator() {
        return new ProjectIterator();
    }

    @Override
    public String str() {
        String columns = "(" + String.join(", ", this.outputColumns) + ")";
        return "Project (cost=" + this.estimateIOCost() + ")" +
                "\n\tcolumns: " + columns;
    }

    @Override
    public TableStats estimateStats() {
        return this.getSource().estimateStats();
    }

    @Override
    public int estimateIOCost() {
        return this.getSource().estimateIOCost();
    }

    private class ProjectIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private boolean hasAgg = false;


        /**
         * 初始化，并且会判断是否有聚合函数
         * */
        private ProjectIterator() {
            this.sourceIterator = ProjectOperator.this.getSource().iterator();
            for (Expression func: expressions) {
                this.hasAgg |= func.hasAgg();
            }
        }

        @Override
        public boolean hasNext() {
            return this.sourceIterator.hasNext();
        }

        @Override

        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record curr = this.sourceIterator.next();

            // 如果没有聚合函数且没有GROUP BY子句，则直接计算并返回结果记录
            if (!this.hasAgg && groupByColumns.size() == 0) {
                List<DataBox> newValues = new ArrayList<>();
                for (Expression f : expressions) {
                    newValues.add(f.evaluate(curr));
                }
                return new Record(newValues);
            }

            // 处理带有聚合函数的情况，需要阻塞直到处理完所有相关记录
            Record base = curr; // 保存第一条记录，用于获取GROUP BY字段的值
            // 持续处理记录直到遇到标记记录（表示一组结束）
            while (curr != GroupByOperator.MARKER) {
                for (Expression dataFunction : expressions) {
                    // 对于聚合表达式，更新其内部状态
                    if (dataFunction.hasAgg()) {
                        dataFunction.update(curr);
                    }
                }
                if (!sourceIterator.hasNext()) break;
                curr = this.sourceIterator.next();
            }

            // 计算最终输出记录的值
            List<DataBox> values = new ArrayList<>();
            for (Expression dataFunction : expressions) {
                if (dataFunction.hasAgg()) {
                    // 对于聚合表达式，计算最终聚合结果并重置状态
                    values.add(dataFunction.evaluate(base));
                    dataFunction.reset();
                } else {
                    // 对于非聚合表达式，直接计算结果
                    values.add(dataFunction.evaluate(base));
                }
            }
            return new Record(values);
        }
    }
}
