# Database API 文档

## Database 类结构图

```mermaid
classDiagram
    class Database {
        - static String METADATA_TABLE_PREFIX
        - static String TABLE_INFO_TABLE_NAME
        - static String INDEX_INFO_TABLE_NAME
        - static int DEFAULT_BUFFER_SIZE
        - static int MAX_SCHEMA_SIZE
        
        - Table tableMetadata
        - Table indexMetadata
        - long numTransactions
        
        - LockManager lockManager
        - DiskSpaceManager diskSpaceManager
        - BufferManager bufferManager
        - RecoveryManager recoveryManager
        
        - int workMem
        - int numMemoryPages
        - Phaser activeTransactions
        - Map~String,TableStats~ stats
        - ArrayList~String~ demoTables
        
        + Database(String fileDir)
        + Database(String fileDir, int numMemoryPages)
        + Database(String fileDir, int numMemoryPages, LockManager lockManager)
        + Database(String fileDir, int numMemoryPages, LockManager lockManager, EvictionPolicy policy)
        + Database(String fileDir, int numMemoryPages, LockManager lockManager, EvictionPolicy policy, boolean useRecoveryManager)
        + close() void
        + waitAllTransactions() void
        + getLockManager() LockManager
        + getDiskSpaceManager() DiskSpaceManager
        + getBufferManager() BufferManager
        + getWorkMem() int
        + setWorkMem(int workMem) void
        + getTableInfoSchema() Schema
        + getIndexInfoSchema() Schema
        + scanTableMetadataRecords() List~Record~
        + scanIndexMetadataRecords() List~Record~
        + beginTransaction() Transaction
        + loadDemo() void
        + loadCSV(String name) boolean
        + dropDemoTables() void
    }
    
    class TableMetadata {
        - String tableName
        - int partNum
        - long pageNum
        - Schema schema
        
        + TableMetadata(String tableName)
        + TableMetadata(Record record)
        + toRecord() Record
    }
    
    class TransactionContextImpl {
        - long transNum
        - Map~String,String~ aliases
        - Map~String,Table~ tempTables
        - long tempTableCounter
        - boolean recoveryTransaction
        
        + getTransNum() long
        + getWorkMemSize() int
        + createTempTable(Schema schema) String
        + deleteAllTempTables() void
        + setAliasMap(Map~String,String~ aliasMap) void
        + clearAliasMap() void
        + indexExists(String tableName, String columnName) boolean
        + updateIndexMetadata(BPlusTreeMetadata metadata) void
        + sortedScan(String tableName, String columnName) Iterator~Record~
        + sortedScanFrom(String tableName, String columnName, DataBox startValue) Iterator~Record~
        + lookupKey(String tableName, String columnName, DataBox key) Iterator~Record~
        + getRecordIterator(String tableName) BacktrackingIterator~Record~
        + contains(String tableName, String columnName, DataBox key) boolean
        + addRecord(String tableName, Record record) RecordId
        + deleteRecord(String tableName, RecordId rid) RecordId
        + getRecord(String tableName, RecordId rid) Record
        + updateRecord(String tableName, RecordId rid, Record updated) RecordId
        + updateRecordWhere(String tableName, String targetColumnName, UnaryOperator~DataBox~ targetValue, String predColumnName, PredicateOperator predOperator, DataBox predValue) void
        + updateRecordWhere(String tableName, String targetColumnName, Function~Record,DataBox~ targetValue, Function~Record,DataBox~ condition) void
        + deleteRecordWhere(String tableName, String predColumnName, PredicateOperator predOperator, DataBox predValue) void
        + deleteRecordWhere(String tableName, Function~Record,DataBox~ condition) void
        + getSchema(String tableName) Schema
        + getFullyQualifiedSchema(String tableName) Schema
        + getStats(String tableName) TableStats
        + getNumDataPages(String tableName) int
        + getTreeOrder(String tableName, String columnName) int
        + getTreeHeight(String tableName, String columnName) int
        + close() void
        + getTable(String tableName) Table
    }
    
    class TransactionImpl {
        - long transNum
        - boolean recoveryTransaction
        - TransactionContext transactionContext
        
        + execute(String statement) Optional~QueryPlan~
        + startCommit() void
        + startRollback() void
        + cleanup() void
        + getTransNum() long
        + createTable(Schema s, String tableName) void
        + dropTable(String tableName) void
        + dropAllTables() void
        + createIndex(String tableName, String columnName, boolean bulkLoad) void
        + dropIndex(String tableName, String columnName) void
        + query(String tableName) QueryPlan
        + query(String tableName, String alias) QueryPlan
        + insert(String tableName, Record values) void
        + update(String tableName, String targetColumnName, UnaryOperator~DataBox~ targetValue) void
        + update(String tableName, String targetColumnName, UnaryOperator~DataBox~ targetValue, String predColumnName, PredicateOperator predOperator, DataBox predValue) void
        + update(String tableName, String targetColumnName, Function~Record,DataBox~ expr, Function~Record,DataBox~ cond) void
        + delete(String tableName, String predColumnName, PredicateOperator predOperator, DataBox predValue) void
        + delete(String tableName, Function~Record,DataBox~ cond) void
        + savepoint(String savepointName) void
        + rollbackToSavepoint(String savepointName) void
        + releaseSavepoint(String savepointName) void
        + getSchema(String tableName) Schema
        + getTransactionContext() TransactionContext
    }
    
    Database --> TableMetadata
    Database --> TransactionContextImpl
    Database --> TransactionImpl
    TransactionContextImpl --> TransactionImpl
```

## 核心API说明

### 构造函数

```mermaid
graph LR
    A[Database] --> B[最简构造函数]
    A --> C[指定内存页数]
    A --> D[指定锁管理器]
    A --> E[指定淘汰策略]
    A --> F[完整参数构造函数]
    
    style A fill:#d8bfd8,stroke:#333
    style B fill:#98fb98,stroke:#333
    style C fill:#98fb98,stroke:#333
    style D fill:#98fb98,stroke:#333
    style E fill:#98fb98,stroke:#333
    style F fill:#98fb98,stroke:#333
```

1. **最简构造函数**
```java
/**
 * 创建一个新的数据库，具有以下默认设置：
 * - 默认缓冲区大小
 * - 锁定禁用（DummyLockManager）
 * - 时钟淘汰策略
 * - 恢复管理器禁用（DummyRecoverManager）
 * @param fileDir 存放表文件的目录
 */
public Database(String fileDir) {
    this (fileDir, DEFAULT_BUFFER_SIZE);
}
```

2. **指定内存页数**
```java
/**
 * 创建一个新的数据库，具有以下默认设置：
 * - 锁定禁用（DummyLockManager）
 * - 时钟淘汰策略
 * - 恢复管理器禁用（DummyRecoverManager）
 * @param fileDir 存放表文件的目录
 * @param numMemoryPages 缓冲区缓存中的内存页数
 */
public Database(String fileDir, int numMemoryPages) {
    this(fileDir, numMemoryPages, new DummyLockManager());
}
```

3. **指定锁管理器**
```java
/**
 * 创建一个新的数据库，具有以下默认设置：
 * - 时钟淘汰策略
 * - 恢复管理器禁用（DummyRecoverManager）
 * @param fileDir 存放表文件的目录
 * @param numMemoryPages 缓冲区缓存中的内存页数
 * @param lockManager 锁管理器
 */
public Database(String fileDir, int numMemoryPages, LockManager lockManager) {
    this(fileDir, numMemoryPages, lockManager, new ClockEvictionPolicy());
}
```

4. **指定淘汰策略**
```java
/**
 * 创建一个新的数据库，恢复功能禁用（DummyRecoveryManager）
 * @param fileDir 存放表文件的目录
 * @param numMemoryPages 缓冲区缓存中的内存页数
 * @param lockManager 锁管理器
 * @param policy 缓冲区缓存的淘汰策略
 */
public Database(String fileDir, int numMemoryPages, LockManager lockManager, EvictionPolicy policy) {
    this(fileDir, numMemoryPages, lockManager, policy, false);
}
```

5. **完整参数构造函数**
```java
/**
 * 创建一个新的数据库。
 * @param fileDir 存放表文件的目录
 * @param numMemoryPages 缓冲区缓存中的内存页数
 * @param lockManager 锁管理器
 * @param policy 缓冲区缓存的淘汰策略
 * @param useRecoveryManager 启用或禁用恢复管理器（ARIES）的标志
 */
public Database(String fileDir, int numMemoryPages, LockManager lockManager, EvictionPolicy policy, boolean useRecoveryManager) {
    boolean initialized = setupDirectory(fileDir);
    numTransactions = 0;
    this.numMemoryPages = numMemoryPages;
    this.lockManager = lockManager;
    
    if (useRecoveryManager) {
        recoveryManager = new ARIESRecoveryManager(this::beginRecoveryTransaction);
    } else {
        recoveryManager = new DummyRecoveryManager();
    }
    
    diskSpaceManager = new DiskSpaceManagerImpl(fileDir, recoveryManager);
    bufferManager = new BufferManager(diskSpaceManager, recoveryManager, numMemoryPages, policy);
    
    // 创建日志分区
    if (!initialized) diskSpaceManager.allocPart(0);
    
    // 执行恢复
    recoveryManager.setManagers(diskSpaceManager, bufferManager);
    if (!initialized) recoveryManager.initialize();
    recoveryManager.restart();
    
    Transaction initTransaction = beginTransaction();
    
    if (!initialized) {
        // _metadata.tables 分区和 _metadata.indices 分区
        diskSpaceManager.allocPart(1);
        diskSpaceManager.allocPart(2);
    }
    if (!initialized) {
        this.initTableInfo();
        this.initIndexInfo();
    } else {
        this.loadMetadataTables();
    }
    initTransaction.commit();
}
```

### 核心管理方法

```mermaid
graph TD
    A[核心管理方法] --> B[资源管理]
    A --> C[事务管理]
    A --> D[元数据管理]
    A --> E[系统配置]
    
    style A fill:#d8bfd8,stroke:#333
    style B fill:#98fb98,stroke:#333
    style C fill:#98fb98,stroke:#333
    style D fill:#98fb98,stroke:#333
    style E fill:#98fb98,stroke:#333
```

#### 资源管理

1. **close()**
```java
/**
 * 关闭数据库
 */
@Override
public synchronized void close() {
    // 等待所有事务终止
    this.waitAllTransactions();
    
    dropDemoTables();
    
    this.bufferManager.evictAll();
    
    this.recoveryManager.close();
    
    this.tableMetadata = null;
    this.indexMetadata = null;
    
    this.bufferManager.close();
    this.diskSpaceManager.close();
}
```

2. **waitAllTransactions()**
```java
// 等待所有事务完成
public synchronized void waitAllTransactions() {
    while (!activeTransactions.isTerminated()) {
        activeTransactions.awaitAdvance(activeTransactions.getPhase());
    }
}
```

#### 事务管理

**beginTransaction()**
```java
/**
 * 开始一个新事务
 * @return 新的Transaction对象
 */
public synchronized Transaction beginTransaction() {
    TransactionImpl t = new TransactionImpl(this.numTransactions, false);
    activeTransactions.register();
    if (activeTransactions.isTerminated()) {
        activeTransactions = new Phaser(1);
    }
    this.recoveryManager.startTransaction(t);
    ++this.numTransactions;
    TransactionContext.setTransaction(t.getTransactionContext());
    return t;
}
```

#### 元数据管理

1. **getTableInfoSchema()**
```java
/**
 * @return _metadata.tables的Schema，包含以下字段:
 *   | 字段名         | 字段类型
 * --+--------------+-------------------------
 * 0 | table_name   | string(32)
 * 1 | part_num     | int
 * 2 | page_num     | long
 * 3 | schema       | byte array(MAX_SCHEMA_SIZE)
 */
public Schema getTableInfoSchema() {
    return new Schema()
            .add("table_name", Type.stringType(32))
            .add("part_num", Type.intType())
            .add("page_num", Type.longType())
            .add("schema", Type.byteArrayType(MAX_SCHEMA_SIZE));
}
```

2. **getIndexInfoSchema()**
```java
/**
 * @return _metadata.indices的Schema，包含以下字段:
 *   | 字段名                | 字段类型
 * --+---------------------+------------
 * 0 | table_name          | string(32)
 * 1 | col_name            | string(32)
 * 2 | order               | int
 * 3 | part_num            | int
 * 4 | root_page_num       | long
 * 5 | key_schema_typeid   | int
 * 6 | key_schema_typesize | int
 * 7 | height              | int
 */
public Schema getIndexInfoSchema() {
    return new Schema()
            .add("table_name", Type.stringType(32))
            .add("col_name", Type.stringType(32))
            .add("order", Type.intType())
            .add("part_num", Type.intType())
            .add("root_page_num", Type.longType())
            .add("key_schema_typeid", Type.intType())
            .add("key_schema_typesize", Type.intType())
            .add("height", Type.intType());
}
```

#### 系统配置

1. **getWorkMem()**
```java
public int getWorkMem() {
    // 限制工作内存不超过内存页数，避免内存溢出错误
    return Math.min(this.workMem, this.numMemoryPages);
}
```

2. **setWorkMem(int workMem)**
```java
public void setWorkMem(int workMem) {
    this.workMem = workMem;
}
```

### 组件访问方法

```mermaid
graph TD
    A[组件访问方法] --> B[锁管理器]
    A --> C[磁盘空间管理器]
    A --> D[缓冲区管理器]
    
    style A fill:#d8bfd8,stroke:#333
    style B fill:#98fb98,stroke:#333
    style C fill:#98fb98,stroke:#333
    style D fill:#98fb98,stroke:#333
```

1. **getLockManager()**
```java
public LockManager getLockManager() {
    return lockManager;
}
```

2. **getDiskSpaceManager()**
```java
public DiskSpaceManager getDiskSpaceManager() {
    return diskSpaceManager;
}
```

3. **getBufferManager()**
```java
public BufferManager getBufferManager() {
    return bufferManager;
}
```

### 数据加载方法

```mermaid
graph TD
    A[数据加载方法] --> B[演示数据]
    A --> C[CSV文件]
    
    style A fill:#d8bfd8,stroke:#333
    style B fill:#98fb98,stroke:#333
    style C fill:#98fb98,stroke:#333
```

1. **loadDemo()**
```java
public void loadDemo() throws IOException {
    demoTables = new ArrayList<>(Arrays.asList("Students", "Courses", "Enrollments"));
    
    dropDemoTables();
    
    for (String table: demoTables) {
        loadCSV(table);
    }
    
    waitAllTransactions();
    getBufferManager().evictAll();
}
```

2. **loadCSV(String name)**
```java
/**
 * 从src/main/resources中加载CSV文件作为表
 * @param name CSV文件名（不包括.csv扩展名）
 * @return 如果表已存在于数据库中则返回true，否则返回false
 */
public boolean loadCSV(String name) throws IOException {
    // 实现细节...
}
```

## 内部类

### TableMetadata
表示表的元数据信息

```java
private static class TableMetadata {
    String tableName;
    int partNum;
    long pageNum;
    Schema schema;
    
    TableMetadata(String tableName) {
        this.tableName = tableName;
        this.partNum = -1;
        this.pageNum = -1;
        this.schema = new Schema();
    }
    
    TableMetadata(Record record) {
        tableName = record.getValue(0).getString();
        partNum = record.getValue(1).getInt();
        pageNum = record.getValue(2).getLong();
        schema = Schema.fromBytes(ByteBuffer.wrap(record.getValue(3).toBytes()));
    }
    
    Record toRecord() {
        byte[] schemaBytes = schema.toBytes();
        byte[] padded = new byte[MAX_SCHEMA_SIZE];
        System.arraycopy(schemaBytes, 0, padded, 0, schemaBytes.length);
        return new Record(tableName, partNum, pageNum, padded);
    }
}
```

### TransactionContextImpl
事务上下文实现类，提供对数据库资源的底层访问

### TransactionImpl
事务实现类，提供面向用户的事务操作接口

## 使用示例

```java
// 创建数据库实例
Database db = new Database("data");

// 开始事务
try (Transaction transaction = db.beginTransaction()) {
    // 创建表
    Schema schema = new Schema()
        .add("id", Type.INT(10))
        .add("name", Type.VARCHAR(50));
    transaction.createTable(schema, "users");
    
    // 插入数据
    List<DataBox> values = Arrays.asList(new IntDataBox(1), new StringDataBox("Alice", 50));
    transaction.insert("users", new Record(values));
    
    // 提交事务
    transaction.commit();
}

// 关闭数据库
db.close();
```