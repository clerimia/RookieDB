package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.query.join.BNLJOperator;
import edu.berkeley.cs186.database.query.join.SNLJOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

/**
 * QueryPlan 提供了一组函数来生成简单的查询。调用与 SQL 语法对应的方法会将信息存储在 QueryPlan 中，
 * 调用 execute 方法会生成并执行一个 QueryPlan DAG。
 */
public class QueryPlan {
    // 事务, 表示查询计划将在这个事务中执行
    private TransactionContext transaction;
    // 表示最终查询计划的查询操作符
    private QueryOperator finalOperator;
    // 要输出的列列表 (SELECT 子句)
    private List<String> projectColumns;
    // 命令行版本使用，用于传递要计算的表达式
    private List<Expression> projectFunctions;
    // 此查询涉及的带别名的表名列表 (FROM 子句)
    private List<String> tableNames;
    // 表示连接的对象列表 (INNER JOIN 子句)
    private List<JoinPredicate> joinPredicates;
    // 从别名到表名的映射 (tableName AS alias)
    private Map<String, String> aliases;
    // WITH 子句中临时表的别名
    private Map<String, String> cteAliases;
    // 表示选择谓词的对象列表 (WHERE 子句)
    private List<SelectPredicate> selectPredicates;
    // 要分组的列列表 (GROUP BY 子句)
    private List<String> groupByColumns;
    // 要排序的列
    private String sortColumn;
    // 产生的记录数限制 (LIMIT 子句)
    private int limit;
    // 产生的记录数偏移量 (OFFSET 子句)
    private int offset;

    /**
     * 在`transaction`中创建一个新的QueryPlan，基表为`baseTableName`
     *
     * @param transaction 包含此查询的事务
     * @param baseTableName 此查询的源表
     */
    public QueryPlan(TransactionContext transaction, String baseTableName) {
        this(transaction, baseTableName, baseTableName);
    }


    /**
     * 在transaction中创建一个新的QueryPlan，基表为startTableName
     * 并将其别名为aliasTableName。
     *
     * @param transaction 包含此查询的事务
     * @param baseTableName 此查询的源表
     * @param aliasTableName 源表的别名
     */
    public QueryPlan(TransactionContext transaction, String baseTableName,
                     String aliasTableName) {
        this.transaction = transaction;

        // 我们的表到目前为止只包含基表
        this.tableNames = new ArrayList<>();
        this.tableNames.add(aliasTableName);

        // 处理别名
        this.aliases = new HashMap<>();
        this.cteAliases = new HashMap<>();
        this.aliases.put(aliasTableName, baseTableName);
        this.transaction.setAliasMap(this.aliases);

        // 随着用户添加project、select等操作，这些将会被填充...
        this.projectColumns = new ArrayList<>();
        this.projectFunctions = null;
        this.joinPredicates = new ArrayList<>();
        this.selectPredicates = new ArrayList<>();
        this.groupByColumns = new ArrayList<>();
        this.limit = -1;
        this.offset = 0;

        // 这将在调用execute()后设置
        this.finalOperator = null;
    }

    public QueryOperator getFinalOperator() {
        return this.finalOperator;
    }


    /**
     * @param column 模棱两可的列名，我们想要确定它所属的表
     * @return 列所属的表名
     * @throws IllegalArgumentException 如果列名存在歧义（即它属于this.tableNames中的两个或多个表）
     * 或者完全未知（即它不属于this.tableNames中的任何表）
     */
    private String resolveColumn(String column) {
        String result = null;
        for (String tableName: this.tableNames) {
            Schema s = transaction.getSchema(tableName);
            for (String fieldName: s.getFieldNames()) {
                if (fieldName.equals(column)) {
                    if (result != null) throw new RuntimeException(
                            "模棱两可的列名 `" + column + "` 同时存在于 `" +
                            result + "` 和 `" + tableName + "` 中。");
                    result = tableName;
                }
            }
        }
        if (result == null)
            throw new IllegalArgumentException("未知的列 `" + column + "`");
        return  result;
    }

    @Override
    public String toString() {
        // Comically large toString() function. Formats the QueryPlan attributes
        // into SQL query format.
        StringBuilder result = new StringBuilder();
        // SELECT clause
        if (this.projectColumns.size() == 0) result.append("SELECT *");
        else {
            result.append("SELECT ");
            result.append(String.join(", ", projectColumns));
        }
        // FROM clause
        String baseTable = this.tableNames.get(0);
        String alias = aliases.get(baseTable);
        if (baseTable.equals(aliases.get(baseTable)))
            result.append(String.format("\nFROM %s\n", baseTable));
        else result.append(String.format("\nFROM %s AS %s\n", baseTable, alias));
        // INNER JOIN clauses
        for (JoinPredicate predicate: this.joinPredicates)
            result.append(String.format("    %s\n", predicate));
        // WHERE clause
        if (selectPredicates.size() > 0) {
            result.append("WHERE\n");
            List<String> predicates = new ArrayList<>();
            for (SelectPredicate predicate: this.selectPredicates) {
                predicates.add(predicate.toString());
            }
            result.append("   ").append(String.join(" AND\n   ", predicates));
            result.append("\n");
        }
        // GROUP BY clause
        if (this.groupByColumns.size() > 0) {
            result.append("GROUP BY ");
            result.append(String.join(", ", groupByColumns));
            result.append("\n");
        }
        result.append(";");
        return result.toString();
    }

    // Helper Classes //////////////////////////////////////////////////////////

    /**
     * 表示单个选择谓词。例如：
     *   table1.col = 186
     *   table2.col <= 123
     *   table3.col > 6
     */
    private class SelectPredicate {
        String tableName;
        String column;
        PredicateOperator operator;
        DataBox value;

        SelectPredicate(String column, PredicateOperator operator, DataBox value) {
            if (column.contains(".")) {
                this.tableName = column.split("\\.")[0];
                column = column.split("\\.")[1];
            }  else {
                this.tableName = resolveColumn(column);
            }

            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("%s.%s %s %s", tableName, column, operator.toSymbol(), value);
        }
    }

    /**
     * 表示查询计划中的等值连接。例如：
     *   INNER JOIN rightTable ON leftTable.leftColumn = rightTable.rightColumn
     *   INNER JOIN table2 ON table2.some_id = table1.some_id
     */
    private class JoinPredicate {
        String leftTable;
        String leftColumn;
        String rightTable;
        String rightColumn;
        private String joinTable; // 仅用于格式化目的

        JoinPredicate(String tableName, String leftColumn, String rightColumn) {
            if (!leftColumn.contains(".") || !rightColumn.contains(".")) {
                throw new IllegalArgumentException("连接列必须完全限定");
            }

            // 下面的分割逻辑只是将列名从表名中分离出来。
            this.joinTable = tableName;
            this.leftTable = leftColumn.split("\\.")[0];
            this.leftColumn = leftColumn;
            this.rightTable = rightColumn.split("\\.")[0];
            this.rightColumn = rightColumn;
            if (!tableName.equals(rightTable) && !tableName.equals(leftTable)) {
                throw new IllegalArgumentException(String.format(
                    "`%s` 无效。INNER JOIN 的 ON 子句必须包含正在连接的新表。",
                    this.toString()
                ));
            }
        }

        @Override
        public String toString() {
            String unAliased = aliases.get(joinTable);
            if (unAliased.equals(joinTable)) {
                return String.format("INNER JOIN %s ON %s = %s",
                    this.joinTable, this.leftColumn, this.rightColumn);
            }
            return String.format("INNER JOIN %s AS %s ON %s = %s",
                unAliased, this.joinTable, this.leftColumn, this.rightColumn);
        }
    }

    // Project /////////////////////////////////////////////////////////////////

    /**
     * 向QueryPlan添加一个投影操作符，指定要投影的列名。
     *
     * @param columnNames 要投影的列
     * @throws RuntimeException 已经指定了投影集合。
     */
    public void project(String...columnNames) {
        project(Arrays.asList(columnNames));
    }

    /**
     * 向QueryPlan添加一个投影操作符，指定要投影的列名列表。只能指定一组投影。
     *
     * @param columnNames 要投影的列
     * @throws RuntimeException 已经指定了投影集合。
     */
    public void project(List<String> columnNames) {
        if (!this.projectColumns.isEmpty()) {
            throw new RuntimeException("不能为此查询添加多个投影操作符。");
        }
        if (columnNames.isEmpty()) {
            throw new RuntimeException("不能不投影任何列。");
        }
        this.projectColumns = new ArrayList<>(columnNames);
    }

    /**
     * 向QueryPlan添加一个投影操作符，指定要投影的列名列表。
     * 带上一些表达式函数
     * */
    public void project(List<String> names, List<Expression> functions) {
        this.projectColumns = names;
        this.projectFunctions = functions;
    }

    /**
     * 将 最终操作符 设置为以 原始最终操作符为源 的 投影操作符。
     * 如果没有投影列，则不执行任何操作。
     */
    private void addProject() {
        if (!this.projectColumns.isEmpty()) {
            if (this.finalOperator == null)
                throw new RuntimeException("无法在空的finalOperator上添加Project。");
            if (this.projectFunctions == null) {
                this.finalOperator = new ProjectOperator(
                        this.finalOperator,
                        this.projectColumns,
                        this.groupByColumns
                );
            } else {
                this.finalOperator = new ProjectOperator(
                        this.finalOperator,
                        this.projectColumns,
                        this.projectFunctions,
                        this.groupByColumns
                );
            }
        }
    }

    // Sort ////////////////////////////////////////////////////////////////////
    /**
     * 向查询计划添加排序操作符，按给定列进行排序。
     */
    public void sort(String sortColumn) {
        if (sortColumn == null) throw new UnsupportedOperationException("只支持一个排序列");
        this.sortColumn = sortColumn;
    }

    /**
     * 如果指定了排序且最终操作符尚未排序，则将最终操作符设置为排序操作符。
     */
    private void addSort() {
        if (this.sortColumn == null) return;
        // 如果列已经排序，则返回
        if (this.finalOperator.sortedBy().contains(sortColumn.toLowerCase())) {
            return; // 已经排序
        }
        // 添加排序操作符
        this.finalOperator = new SortOperator(
                this.transaction,
                this.finalOperator,
                this.sortColumn
        );
    }

    // Limit ///////////////////////////////////////////////////////////////////

    /**
     * 添加一个没有偏移量的限制
     * @param limit 要生成的记录数的上限
     */
    public void limit(int limit) {
        this.limit(limit, 0);
    }

    /**
     * 添加一个带偏移量的限制
     * @param limit 要生成的记录数的上限
     * @param offset 在生成第一条记录之前丢弃的记录数
     */
    public void limit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    /**
     * 如果limit为非负数，则将最终操作符设置为以原始最终操作符为源的limit操作符。
     */
    private void addLimit() {
        if (this.limit >= 0) {
            this.finalOperator = new LimitOperator(
                    this.finalOperator,
                    this.limit,
                    this.offset
            );
        }
    }

    // Select //////////////////////////////////////////////////////////////////

    /**
     * 添加一个选择操作符。只返回满足相对于value的谓词条件的列。
     *
     * @param column 指定谓词的列
     * @param operator 谓词的操作符 (=, <, <=, >, >=, !=)
     * @param value 用于比较的值
     */
    public void select(String column,
                       PredicateOperator operator,
                       Object value) {
        DataBox d = DataBox.fromObject(value);
        this.selectPredicates.add(new SelectPredicate(column, operator, d));
    }

    /**
     * 对于每个选择谓词：
     * - 创建一个以最终操作符为源的选择操作符
     * - 将当前的最终操作符设置为新的选择操作符
     */
    private void addSelectsNaive() {
        for (int i = 0; i < selectPredicates.size(); i++) {
            SelectPredicate predicate = selectPredicates.get(i);
            this.finalOperator = new SelectOperator(
                    this.finalOperator,
                    predicate.tableName + "." + predicate.column,
                    predicate.operator,
                    predicate.value
            );
        }
    }

    // Group By ////////////////////////////////////////////////////////////////

    /**
     * 为查询设置分组列。
     *
     * @param columns 要分组的列
     */
    public void groupBy(String...columns) {
        this.groupByColumns = Arrays.asList(columns);
    }

    /**
     * 为查询设置分组列。
     *
     * @param columns 要分组的列
     */
    public void groupBy(List<String> columns) {
        this.groupByColumns = columns;
    }

    /**
     * 将最终操作符设置为以原始最终操作符为源的GroupByOperator。
     * 如果没有分组列，则不执行任何操作。
     */
    private void addGroupBy() {
        if (this.groupByColumns.size() > 0) {
            if (this.finalOperator == null) throw new RuntimeException(
                    "无法在空的finalOperator上添加GroupBy。"
            );
            this.finalOperator = new GroupByOperator(
                    this.finalOperator,
                    this.transaction,
                    this.groupByColumns
            );
        }
    }

    // Join ////////////////////////////////////////////////////////////////////

    /**
     * 将现有查询计划的 leftColumnName 列与 tableName 表的 rightColumnName 列进行连接。
     *
     * @param tableName 要连接的表
     * @param leftColumnName 现有 QueryPlan 中的连接列
     * @param rightColumnName tableName 中的连接列
     */
    public void join(String tableName, String leftColumnName, String rightColumnName) {
        join(tableName, tableName, leftColumnName, rightColumnName);
    }

    /**
     * 将现有查询计划的 leftColumnName 列与 tableName 表的 rightColumnName 列进行连接，
     * 并将 tableName 表别名为 aliasTableName。
     *
     * baseTable join tableName as aliasTableName On predicates
     *
     * @param tableName 要连接的表
     * @param aliasTableName 要连接的表的别名
     * @param leftColumnName 现有 QueryPlan 中的连接列
     * @param rightColumnName tableName 中的连接列
     */
    public void join(String tableName, String aliasTableName, String leftColumnName, String rightColumnName) {
        // 检查别名，如果已存在，则抛出异常
        if (this.aliases.containsKey(aliasTableName)) {
            throw new RuntimeException("table/alias " + aliasTableName + " 已在使用中");
        }
        if (cteAliases.containsKey(tableName)) {
            tableName = cteAliases.get(tableName);
        }
        this.aliases.put(aliasTableName, tableName);

        // 添加连接谓词
        this.joinPredicates.add(
                new JoinPredicate(
                    aliasTableName,
                    leftColumnName,
                    rightColumnName
        ));

        // 添加表名
        this.tableNames.add(aliasTableName);

        // 设置别名
        this.transaction.setAliasMap(this.aliases);
    }

    /**
     * 对于 this.joinTableNames 中的每个表：
     * - 遍历JoinPredicates: 保存了要连接的所有信息，然后创建简单嵌套循环连接，基表使用顺序扫描
     * - 创建该表的顺序扫描操作符
     * - 将当前的最终操作符与顺序扫描操作符进行连接
     * - 将最终操作符设置为连接操作符
     */
    private void addJoinsNaive() {
        int pos = 1;
        for (JoinPredicate predicate : joinPredicates) {
            this.finalOperator = new SNLJOperator(
                    finalOperator,
                    new SequentialScanOperator( // 创建该表的顺序扫描操作符，体现了左深连接树
                            this.transaction,
                            tableNames.get(pos)
                    ),
                    predicate.leftColumn,
                    predicate.rightColumn,
                    this.transaction
            );
            pos++;
        }
    }

    /**
     * 添加临时表的别名
     * */
    public void addTempTableAlias(String tableName, String alias) {
        if (cteAliases.containsKey(alias)) {
            throw new UnsupportedOperationException("重复的别名 " + alias);
        }
        cteAliases.put(alias, tableName);
        for (String k: aliases.keySet()) {
            if (aliases.get(k).toLowerCase().equals(alias.toLowerCase())) {
                aliases.put(k, tableName);
            }
        }
        this.transaction.setAliasMap(this.aliases);
    }

    // Task 5: Single Table Access Selection 单表访问选择 ///////////////////////////////////
    /**
     * 获取给定表中可以使用索引扫描的所有选择谓词的索引位置。
     * 
     * 一个谓词可以使用索引扫描需要满足以下条件：
     * 1. 该谓词作用于指定的表
     * 2. 该表的对应列上存在索引
     * 3. 操作符不是NOT_EQUALS（因为不等于操作无法有效利用索引）
     *
     * @param table 表名
     * @return 包含可使用索引扫描的选择谓词在selectPredicates列表中索引位置的列表
     */
    private List<Integer> getEligibleIndexColumns(String table) {
        List<Integer> result = new ArrayList<>();
        
        // 遍历所有选择谓词
        for (int i = 0; i < this.selectPredicates.size(); i++) {
            SelectPredicate p = this.selectPredicates.get(i);
            // 忽略针对不同表的选择谓词
            if (!p.tableName.equals(table)) continue;
            
            // 检查该列是否存在索引
            boolean indexExists = this.transaction.indexExists(table, p.column);
            // 检查操作符是否可以使用索引扫描
            boolean canScan = p.operator != PredicateOperator.NOT_EQUALS;
            if (indexExists && canScan) result.add(i);
        }
        return result;
    }

    /**
     * 将所有适用的选择谓词应用于给定的源操作符，除了指定索引的谓词。
     * 这个体现出谓词下推的优化思想
     * 
     * 在使用索引扫描时，用于索引扫描的选择谓词已经应用过了，
     * 因此不需要再次应用，这就是 except 参数存在的原因。
     * 
     * 这体现了System R查询优化中的谓词下推（Predicate Pushdown）优化技术：
     * 将选择条件下推到尽可能靠近数据源的位置执行，减少中间结果集的大小，
     * 从而提高查询性能。同时也体现了System R的剪枝（Pruning）原理，
     * 尽早过滤掉不满足条件的元组。
     * 
     * @param source 要应用选择条件的源操作符
     * @param except 要跳过的选择条件的索引，如果不需要跳过任何条件则传入 -1
     * @return 应用选择谓词后的新查询操作符
     */
    private QueryOperator addEligibleSelections(QueryOperator source, int except) {
        // 遍历执行计划的所有谓词
        for (int i = 0; i < this.selectPredicates.size(); i++) {

            // 已经在索引中使用过了
            if (i == except) continue;

            // 获取当前选择谓词
            SelectPredicate curr = this.selectPredicates.get(i);
            try {

                // 获取当前选择谓词的列名
                String colName = source.getSchema().matchFieldName(curr.tableName + "." + curr.column);
                // 应用选择算子到当前操作符
                source = new SelectOperator(
                        source, colName, curr.operator, curr.value
                );
            } catch (RuntimeException err) {
                /* 什么都不做 */
            }
        }
        return source;
    }

    /**
     * 查找访问给定表的最低成本 QueryOperator。首先确定给定表的顺序扫描成本。
     * 然后对于该表上每个可用的索引，确定索引扫描的成本。跟踪最小成本操作并下推符合条件的选择谓词。
     *
     * 如果选择了索引扫描，在下推选择条件时排除冗余的选择谓词。
     * 此方法将在搜索算法的第一遍中调用，以确定访问每个表的最有效方式。
     *
     * @return 一个具有扫描给定表最低成本的 QueryOperator，它可以是 SequentialScanOperator
     * 或嵌套在任何可能的下推选择操作符中的 IndexScanOperator。最低成本操作符的平局可以任意打破。
     * 这个函数就是给一个表名，然后返回一个访问该表的最优操作符
     */
    public QueryOperator minCostSingleAccess(String table) {
        QueryOperator minOp = new SequentialScanOperator(this.transaction, table);

        // TODO(proj3_part2): implement
        return minOp;
    }

    // Task 6: 连接选择 //////////////////////////////////////////////////

    /**
     * 运用开销最小的连接操作符
     * 给定左右操作符之间的连接谓词，从 JoinOperator.JoinType 中找到成本最低的连接操作符。
     * 默认情况下只考虑 SNLJ 和 BNLJ，以防止依赖 GHJ、Sort 和 SMJ。
     *
     * 提醒：您的实现不需要考虑笛卡尔积，也不需要跟踪有趣的排序。
     *
     * @return 输入操作符之间成本最低的连接 QueryOperator
     */
    private QueryOperator minCostJoinType(QueryOperator leftOp,     // 左源操作符
                                          QueryOperator rightOp,    // 右源操作符
                                          String leftColumn,        // 左属性名
                                          String rightColumn        // 右属性名
    ) {
        // 作用后两个操作符变成一个Join操作符
        QueryOperator bestOperator = null;

        // 开始选择最小成本的
        int minimumCost = Integer.MAX_VALUE;
        List<QueryOperator> allJoins = new ArrayList<>();
        // 仅考虑 SNLJ BNLJ
        allJoins.add(new SNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
        allJoins.add(new BNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
        for (QueryOperator join : allJoins) {
            int joinCost = join.estimateIOCost();
            if (joinCost < minimumCost) {
                bestOperator = join;
                minimumCost = joinCost;
            }
        }
        return bestOperator;
    }

    /**
     * 遍历搜索的前一遍中的所有表集合。对于每个表集合，检查每个连接谓词看是否存在与新表的有效连接。
     * 如果存在，则找到成本最低的连接。返回一个从正在连接的每个表名集合到其最低成本连接操作符的映射。
     *
     * 连接谓词存储为 `this.joinPredicates` 的元素。
     *
     * @param prevMap  将表集合映射到该表集合上的查询操作符。每个集合应该有 pass number - 1 个元素。
     * @param pass1Map 每个集合恰好包含一个表，映射到单个表访问（扫描）查询操作符。
     * @return 一个从表名到连接 QueryOperator 的映射。每个表名集合中的元素数量应该等于遍历次数。
     */
    public Map<Set<String>, QueryOperator> minCostJoins(
            Map<Set<String>, QueryOperator> prevMap,
            Map<Set<String>, QueryOperator> pass1Map) {
        Map<Set<String>, QueryOperator> result = new HashMap<>();
        // TODO(proj3_part2): implement
        // We provide a basic description of the logic you have to implement:
        // For each set of tables in prevMap
        //   For each join predicate listed in this.joinPredicates
        //      Get the left side and the right side of the predicate (table name and column)
        //
        //      Case 1: The set contains left table but not right, use pass1Map
        //              to fetch an operator to access the rightTable
        //      Case 2: The set contains right table but not left, use pass1Map
        //              to fetch an operator to access the leftTable.
        //      Case 3: Otherwise, skip this join predicate and continue the loop.
        //
        //      Using the operator from Case 1 or 2, use minCostJoinType to
        //      calculate the cheapest join with the new table (the one you
        //      fetched an operator for from pass1Map) and the previously joined
        //      tables. Then, update the result map if needed.
        return result;
    }

    // Task 7: 最优计划选择 //////////////////////////////////////////

    /**
     * 在给定映射中查找成本最低的 QueryOperator。映射是在搜索算法的每次遍历中生成的，
     * 并将表集合关联到访问这些表的成本最低的 QueryOperator。
     *
     * @return 给定映射中的一个 QueryOperator
     */
    private QueryOperator minCostOperator(Map<Set<String>, QueryOperator> map) {
        if (map.size() == 0) throw new IllegalArgumentException(
                "无法在空映射上找到最小成本操作符"
        );
        QueryOperator minOp = null;
        int minCost = Integer.MAX_VALUE;
        for (Set<String> tables : map.keySet()) {
            QueryOperator currOp = map.get(tables);
            int currCost = currOp.estimateIOCost();
            if (currCost < minCost) {
                minOp = currOp;
                minCost = currCost;
            }
        }
        return minOp;
    }

    /**
     * 基于 System R 成本基础查询优化器生成优化的 QueryPlan。
     *
     * @return 作为此查询结果的记录迭代器
     */
    public Iterator<Record> execute() {
        this.transaction.setAliasMap(this.aliases);
        // TODO(proj3_part2): implement
        // Pass 1: For each table, find the lowest cost QueryOperator to access
        // the table. Construct a mapping of each table name to its lowest cost
        // operator.
        //
        // Pass i: On each pass, use the results from the previous pass to find
        // the lowest cost joins with each table from pass 1. Repeat until all
        // tables have been joined.
        //
        // Set the final operator to the lowest cost operator from the last
        // pass, add group by, project, sort and limit operators, and return an
        // iterator over the final operator.
        return this.executeNaive(); // TODO(proj3_part2): Replace this!
    }

    // 执行简单查询 ///////////////////////////////////////////////////////////
    // 以下函数用于生成简单的查询计划。您可以参考它们获取指导，
    // 但在实现自己的 execute 函数时不需要使用这些方法。

    /**
     * 给定一个没有连接的单表简单查询，例如：
     *      SELECT * FROM table WHERE table.column >= 186;
     *
     * 我们可以利用 table.column 上的索引来只扫描满足谓词的值。
     * 此函数确定是否存在我们可以执行此优化的列。
     *
     * @return 如果未找到符合条件的选择谓词则返回 -1，否则返回符合条件的选择谓词的索引。
     */
    private int getEligibleIndexColumnNaive() {
        boolean hasGroupBy = this.groupByColumns.size() > 0;
        boolean hasJoin = this.joinPredicates.size() > 0;
        if (hasGroupBy || hasJoin) return -1;
        for (int i = 0; i < selectPredicates.size(); i++) {
            // 对于每个选择谓词，检查我们是否在谓词的列上有索引。
            // 如果谓词操作符是我们可以执行扫描的操作符（=, >=, >, <=, <），
            // 则返回符合条件谓词的索引
            SelectPredicate predicate = selectPredicates.get(i);
            boolean hasIndex = this.transaction.indexExists(
                    this.tableNames.get(0), predicate.column
            );
            if (hasIndex && predicate.operator != PredicateOperator.NOT_EQUALS) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 生成一个利用 `indexPredicate` 列索引的单表查询计划。
     *
     * @param indexPredicate 我们可以在索引扫描中使用的 select 谓词的索引。
     */
    private void generateIndexPlanNaive(int indexPredicate) {
        SelectPredicate predicate = this.selectPredicates.get(indexPredicate);
        this.finalOperator = new IndexScanOperator(
                this.transaction, this.tableNames.get(0),
                predicate.column,
                predicate.operator,
                predicate.value
        );
        this.selectPredicates.remove(indexPredicate);
        this.addSelectsNaive();
        this.addProject();
    }

    /**
     * 生成一个简单的 QueryPlan，其中所有连接都在 DAG 的底部，
     * 接着是所有选择谓词，可选的 group by 操作符，可选的 project 操作符，
     * 可选的 sort 操作符和可选的 limit 操作符（按此顺序）。
     *
     * @return 作为此查询结果的记录迭代器
     */
    public Iterator<Record> executeNaive() {
        this.transaction.setAliasMap(this.aliases);
        int indexPredicate = this.getEligibleIndexColumnNaive();
        if (indexPredicate != -1) {
            this.generateIndexPlanNaive(indexPredicate);
        } else {
            // 从第一个表的扫描开始
            this.finalOperator = new SequentialScanOperator(
                    this.transaction,
                    this.tableNames.get(0)
            );

            // 向我们的计划添加连接、选择、分组和投影
            this.addJoinsNaive();
            this.addSelectsNaive();
            this.addGroupBy();
            this.addProject();
            this.addSort();
            this.addLimit();
        }
        return this.finalOperator.iterator();
    }

}
