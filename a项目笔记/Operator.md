## QueryOperator 
所有操作符的抽象类, 实现了可迭代接口，泛型是Record:物理记录  
### 状态、属性
- QueryOperator source: 源操作符
- Schema outputSchema: 输出的Schema
- TableStats stats: 表的统计信息、CataLog
- OperatorType type: 操作符类型
```java
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
```

### API
#### 1.构造函数
- 创建一个仅有类型的算子
```java
public QueryOperator(OperatorType type){}
```

- 创建一个设置了源的算子， 计算输出模式this.computeSchema()
```java
protected QueryOperator(OperatorType type, QueryOperator source) {}
```

#### 2.子类实现函数
- get/is/set 方法: 获取或者设置各种状态和属性
- sortedBy() : 返回一个空集合, 使用需要子类重写
```java
    public List<String> sortedBy() {
        return Collections.emptyList();
    }
```
- 计算操作符的输出模式
```java
protected abstract Schema computeSchema();
```
- 获取操作符的迭代器
```java
public abstract Iterator<Record> iterator();
```
- 是否物化
```java
    /**
     * @return 如果此查询操作符的记录物化在表中则返回true。
     */
    public boolean materialized() {
        return false;
    }
```
- 获取回溯迭代器
```java
    /**
     * @throws UnsupportedOperationException 如果此操作符不支持回溯
     * @return 此操作符记录的回溯迭代器
     */
    public BacktrackingIterator<Record> backtrackingIterator() {
        throw new UnsupportedOperationException(
            "此操作符不支持回溯。您可能想要先使用QueryOperator.materialize。"
        );
    }
```
- 估计操作符执行结果的统计信息
```java
    /**
     * 估计执行此查询操作符的结果的表统计信息。
     *
     * @return 估计的TableStats
     */
    public abstract TableStats estimateStats();
```
- 估计执行操作符的IO成本
```java
    /**
     * 估计执行此查询操作符的IO成本。
     *
     * @return 估计的IO操作次数
     */
    public abstract int estimateIOCost();
```

#### 3.静态方法
- 从一个记录迭代器中提取指定页数的记录，并返回一个支持回溯的迭代器
- 其实就是将内存加载到内存中，变成一个数组，返回一个回溯的数组迭代器
```java
    /**
     * @param records  输入的记录迭代器（数据源）
     * @param schema   记录的模式
     * @param maxPages 要消耗的记录的最大页数
     * @return 从一个记录迭代器中提取指定页数的记录，并返回一个支持回溯的迭代器
     */
    public static BacktrackingIterator<Record> getBlockIterator(Iterator<Record> records, Schema schema, int maxPages) {
        int recordsPerPage = Table.computeNumRecordsPerPage(PageDirectory.EFFECTIVE_PAGE_SIZE, schema);
        int maxRecords = recordsPerPage * maxPages;
        List<Record> blockRecords = new ArrayList<>();
        for (int i = 0; i < maxRecords && records.hasNext(); i++) {
            blockRecords.add(records.next());
        }
        return new ArrayBacktrackingIterator<>(blockRecords);
    }
```
- 物化一个查询操作符，有一些操作符不支持回溯，物化了就可以回溯了
```java
    /**
     * @param operator 要物化的查询操作符
     * @param transaction 物化表将在其中创建的事务
     * @return 从`operator`记录中提取的新MaterializedOperator
     */
    public static QueryOperator materialize(QueryOperator operator, TransactionContext transaction) {
        if (!operator.materialized()) {
            return new MaterializeOperator(operator, transaction);
        }
        return operator;
    }
```

## JoinOperator
这个连接操作仅支持等值连接  
1.80/20原则：80%的连接查询都是简单的等值连接  
2.性能优化：等值连接可以使用各种高效的算法  
3.实现简单：避免了处理复杂条件的开销  
4.组合性好：可以通过组合基本操作实现复杂查询  
### 状态、属性
- JoinType 连接方式
- QueryOperator 源操作符
- ColumnIndex 连接列索引 为什么是int?
- ColumnName 连接列名称
- transaction 当前事务

### API
#### 1. 构造函数
```java
    public JoinOperator(
            QueryOperator leftSource,  // 左源算子
            QueryOperator rightSource, // 右源算子
            String leftColumnName,     // 左列名称
            String rightColumnName,    // 右列名称
            TransactionContext transaction, // 事务上下文
            JoinType joinType          // 连接方式
    ){}
```

#### 2. 实现的QueryOperator方法
- public QueryOperator getSource()  无单一源，直接抛出异常
- public Schema computeSchema() 计算结果的表结构、使用了concat方法
```java
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
```
- public TableStats estimateStats() 估计结果表的统计信息 调用了TableStats的copyWithJoin方法
- public int compare(Record leftRecord, Record rightRecord) 输入两个记录,比较两个记录在连接列上的值
```java
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
```

## ScanOperator
### SequentialScanOperator 
这是一个基表操作符，用于顺序扫描
#### 属性
- transaction 事务上下文
- tableName   基表名称
#### 1.构造函数
- 创建一个基表的扫描操作符，枚举类型为SEQ_SCAN，会计算Schema
```java
protected SequentialScanOperator(
        OperatorType type, // SEQ_SCAN
        TransactionContext transaction, 
        String tableName
) {
    super(type);
    this.transaction = transaction;
    this.tableName = tableName;
    this.setOutputSchema(this.computeSchema());
    
    this.stats = this.estimateStats();
}
```
#### 2.核心API
- iterator: 获取一个回溯迭代器
- materialized: 返回true，是物化操作符
- backtrackingIterator(): 通过事务上下文，输入表名获取可回溯迭代器
- computeSchema(): 通过事务上下文获取结果表的模式
- estimateStats(): 通过事务上下文结果表的统计信息
- estimateIOCost(): 通过事务上下文结果表的IO成本

### IndexScanOperator 
索引扫描操作符，体现了谓词下推，只有有谓词的版本
#### 1.属性和构造函数
- transaction: 事务上下文
- tableName:  基表名称
- columnName: 索引列名称
- predicate:  索引列的查询条件, 谓词
- value:      索引列的查询条件值
- columnIndex 索引列在Schema列表中的位置
```java
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
        super(OperatorType.INDEX_SCAN); // INDEX_SCAN
        this.tableName = tableName;
        this.transaction = transaction;
        this.columnName = columnName;
        this.predicate = predicate;
        this.value = value;
        this.setOutputSchema(this.computeSchema());
        this.columnIndex = this.getSchema().findField(columnName);
        this.stats = this.estimateStats();
    }
```

#### 2.核心API
- estimateStats(): 通过事务上下文获取结果表的统计信息, 调用了TableStats的copyWithPredicate方法
- estimateIOCost(): 通过事务上下文获取结果表的IO成本, 调用了TableStats的estimateIOCost方法
```java
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
```