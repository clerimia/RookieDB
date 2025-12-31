# RookieDB - 关系型数据库管理系统

一个功能完整的关系型数据库管理系统实现，基于 UC Berkeley CS186 数据库系统课程项目。本项目实现了现代数据库系统的核心功能，包括 B+ 树索引、查询优化、并发控制和崩溃恢复机制。

## ✨ 核心特性

- 🌲 **B+ 树索引**: 高效的数据索引和范围查询支持
- 🔄 **查询优化**: 基于成本的动态规划优化器，自动选择最优执行计划
- 🔒 **并发控制**: 实现多粒度锁协议和严格两阶段锁（Strict 2PL）
- 💾 **崩溃恢复**: 完整的 ARIES 恢复算法实现，支持 WAL 日志
- 🔥 **火山模型**: 流式查询执行引擎，支持多种连接算法
- 🗄️ **缓冲管理**: LRU/Clock 页面替换策略
- 📊 **SQL 支持**: 完整的 SQL 解析器和执行引擎

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────┐
│            客户端层 (Client Layer)               │
│              Python Client (client.py)          │
└──────────────────┬──────────────────────────────┘
                   │ TCP Socket (Port 18600)
┌──────────────────▼──────────────────────────────┐
│          连接层 (Connection Layer)               │
│              Server / CLI Interface             │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│          SQL处理层 (SQL Processing)              │
│     Parser → Visitor → QueryPlan → Optimizer    │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│          执行引擎层 (Execution Engine)           │
│       QueryOperators (BNLJ/GHJ/SMJ/Sort)        │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│          事务层 (Transaction Layer)              │
│    Transaction / TransactionContext / ACID      │
├──────────────┬───────────────┬──────────────────┤
│  并发控制    │   恢复管理     │   存储引擎       │
│  LockMgr     │  RecoveryMgr  │  Table/Index    │
└──────────────┴───────────────┴──────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│         缓冲管理器 (Buffer Manager)              │
│           LRU / Clock Eviction Policy           │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│         磁盘管理器 (Disk Space Manager)          │
│              Partition / Page I/O               │
└─────────────────────────────────────────────────┘
```

## 🚀 快速开始

### 前置要求

- Java 11 或更高版本
- Maven 3.6+
- Docker（可选，用于容器化部署）

### 本地运行

```bash
# 克隆项目
git clone <repository-url>
cd sp25-rookiedb

# 编译项目
mvn clean compile

# 运行测试
mvn test

# 启动数据库服务器
mvn exec:java -Dexec.mainClass="edu.berkeley.cs186.database.cli.Server"
```

### Docker 部署

**前提条件**：
- 确保 Docker Desktop 已启动并正常运行
- 本地已完成项目构建（生成 JAR 文件）

#### 步骤一：本地构建项目

```bash
# 编译并打包项目（需要先在本地构建）
mvn clean package -DskipTests
   
```

#### 步骤二：使用 Docker Compose（推荐）

```bash
# 启动服务（自动构建镜像）
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f rookiedb

# 停止服务
docker-compose stop

# 启动服务
docker-compose start

# 重启服务
docker-compose restart

# 停止并删除容器（保留数据卷）
docker-compose down

# 停止并删除容器和数据卷
docker-compose down -v

# 重新构建并启动
docker-compose up -d --build
```

#### 步骤三：直接使用 Docker（可选）

```bash
# 1. 构建镜像（第一次构建可能需要几分钟）
docker build -t rookiedb .

# 2. 运行容器（端口映射到宿主机）
docker run -d -p 18600:18600 --name rookiedb-server rookiedb

# 3. 查看容器状态
docker ps

# 4. 查看日志
docker logs rookiedb-server

# 5. 停止容器
docker stop rookiedb-server

# 6. 重新启动容器
docker start rookiedb-server

# 7. 删除容器
docker rm -f rookiedb-server
```

**使用数据卷持久化数据**：
```bash
# 创建数据卷并运行
docker run -d -p 18600:18600 \
  -v rookiedb-data:/app/demo \
  --name rookiedb-server \
  rookiedb
```

**自定义 JVM 参数**：
```bash
docker run -d -p 18600:18600 \
  -e JAVA_OPTS="-Xms512m -Xmx1g" \
  --name rookiedb-server \
  rookiedb
```

#### 配置说明

**docker-compose.yml 配置项**：
- `ports`: 端口映射 18600:18600
- `environment`: JVM 内存配置
- `volumes`: 数据持久化到命名卷 `rookiedb-data`
- `restart`: 容器自动重启策略
- `networks`: 独立网络隔离

### 连接到数据库

启动数据库服务器后，在本机使用 Python 客户端连接：

```bash
# 进入项目根目录
cd sp25-rookiedb

# 运行 Python 客户端连接数据库
python client.py
```

> **注意**：需要 Python 3 环境，client.py 位于项目根目录

## 🚀 支持的 SQL 语句

> 本文档基于 `RookieParser.jjt` 解析器定义和相关 Java 实现整理

### 1. 数据类型

RookieDB 支持以下基本数据类型（详见 `TypeId.java`）：

| 类型 | 描述 | 字节数 | 示例 |
|------|------|--------|------|
| **BOOL** | 布尔类型 | 1 | `true`, `false` |
| **INT** | 32 位整数 | 4 | `42`, `-100` |
| **LONG** | 64 位整数 | 8 | `9223372036854775807` |
| **FLOAT** | 单精度浮点数 | 4 | `3.14`, `-0.5` |
| **STRING(n)** | 固定长度字符串 | n | `'hello'` (n=5) |
| **BYTE_ARRAY(n)** | 字节数组 | n | 二进制数据 |

**重要限制：**
- ❌ 不支持 NULL 值（所有字段必须有值）
- ❌ 不支持约束（PRIMARY KEY、FOREIGN KEY、UNIQUE、CHECK、NOT NULL）
- ✅ 数值类型使用 Java 基本类型（int、long、float），无法表示空值

### 2. 表达式与操作符

#### 2.1 聚合函数（Aggregate Functions）

详见 `AggregateFunction.java` 实现。

**统计类聚合函数**

| 函数 | 说明 | 适用类型 | 返回类型 | 示例 |
|------|------|----------|----------|------|
| `COUNT(*)` | 计数所有行 | 所有类型 | INT | `SELECT COUNT(*) FROM t` |
| `COUNT(column)` | 计数非空列 | 所有类型 | INT | `SELECT COUNT(id) FROM t` |
| `SUM(column)` | 求和 | BOOL, INT, LONG, FLOAT | 同输入类型 | `SELECT SUM(salary) FROM emp` |
| `AVG(column)` | 平均值 | INT, LONG, FLOAT | FLOAT | `SELECT AVG(age) FROM users` |
| `MIN(column)` | 最小值 | 所有类型 | 同输入类型 | `SELECT MIN(price) FROM products` |
| `MAX(column)` | 最大值 | 所有类型 | 同输入类型 | `SELECT MAX(score) FROM tests` |

**统计分析函数**

| 函数 | 说明 | 适用类型 | 返回类型 | 公式 |
|------|------|----------|----------|------|
| `RANGE(column)` | 极差（最大值-最小值） | INT, LONG, FLOAT | 同输入类型 | MAX - MIN |
| `VAR(column)` | 方差 | INT, LONG, FLOAT | FLOAT | σ² |
| `STDDEV(column)` | 标准差 | INT, LONG, FLOAT | FLOAT | √σ² |

**序列类聚合函数**

| 函数 | 说明 | 适用类型 | 返回类型 |
|------|------|----------|----------|
| `FIRST(column)` | 返回第一行的值 | 所有类型 | 同输入类型 |
| `LAST(column)` | 返回最后一行的值 | 所有类型 | 同输入类型 |
| `RANDOM(column)` | 随机返回一个值 | 所有类型 | 同输入类型 |

**聚合函数使用限制**

- ❌ 不支持嵌套聚合函数（如 `SUM(AVG(x))` 不支持）
- ❌ 每个聚合函数只接受一个参数（COUNT 可使用 `*`）
- ❌ STRING 和 BYTE_ARRAY 不支持数值运算函数（SUM、AVG、VAR、STDDEV、RANGE）
- ❌ 不支持 DISTINCT 关键字（如 `COUNT(DISTINCT col)` 不支持）

#### 2.2 单行函数（Scalar Functions）

详见 `NamedFunction.java` 实现。

**字符串函数**

| 函数 | 说明 | 参数类型 | 返回类型 | 示例 |
|------|------|----------|----------|------|
| `UPPER(str)` | 转大写 | STRING | STRING | `UPPER('hello')` → `'HELLO'` |
| `LOWER(str)` | 转小写 | STRING | STRING | `LOWER('WORLD')` → `'world'` |
| `REPLACE(str, from, to)` | 字符串替换 | STRING × 3 | STRING | `REPLACE('hello', 'l', 'r')` → `'herro'` |

**数学函数**

| 函数 | 说明 | 适用类型 | 返回类型 | 示例 |
|------|------|----------|----------|------|
| `FLOOR(num)` | 向下取整 | INT, LONG, FLOAT | LONG | `FLOOR(3.7)` → `3` |
| `CEIL(num)` | 向上取整 | INT, LONG, FLOAT | LONG | `CEIL(3.2)` → `4` |
| `ROUND(num)` | 四舍五入 | INT, LONG, FLOAT | LONG | `ROUND(3.5)` → `4` |
| `NEGATE(num)` | 取负数 | INT, LONG, FLOAT | 同输入类型 | `NEGATE(5)` → `-5` |

**函数使用限制**

- ❌ 字符串函数不适用于数值类型
- ❌ 数学函数不支持 STRING、BYTE_ARRAY、BOOL 类型
- ✅ 可以在 SELECT、WHERE 等子句中使用

#### 2.3 操作符（Operators）

**比较操作符**

| 操作符 | 说明 | 示例 | 备注 |
|--------|------|------|------|
| `=`, `==` | 等于 | `age = 18` | 两种写法等价 |
| `!=`, `<>` | 不等于 | `status != 'active'` | 两种写法等价 |
| `<` | 小于 | `price < 100` | |
| `<=` | 小于等于 | `age <= 65` | |
| `>` | 大于 | `salary > 5000` | |
| `>=` | 大于等于 | `score >= 60` | |

**逻辑操作符**

| 操作符 | 说明 | 示例 | 优先级 |
|--------|------|------|--------|
| `NOT`, `!` | 逻辑非 | `NOT active` | 高 |
| `AND`, `&&` | 逻辑与 | `age > 18 AND age < 65` | 中 |
| `OR`, `\|\|` | 逻辑或 | `status = 'A' OR status = 'B'` | 低 |

**算术操作符**

| 操作符 | 说明 | 示例 | 适用类型 |
|--------|------|------|----------|
| `+` | 加法 | `price + tax` | INT, LONG, FLOAT |
| `-` | 减法 | `total - discount` | INT, LONG, FLOAT |
| `*` | 乘法 | `quantity * price` | INT, LONG, FLOAT |
| `/` | 除法 | `total / count` | INT, LONG, FLOAT |
| `%` | 取模 | `id % 10` | INT, LONG, FLOAT |

**运算优先级（从高到低）**

1. 括号 `()`
2. 一元操作符（`-`, `NOT`）
3. 乘除模 (`*`, `/`, `%`)
4. 加减 (`+`, `-`)
5. 比较操作符 (`=`, `<`, `>`, 等)
6. 逻辑非 (`NOT`)
7. 逻辑与 (`AND`)
8. 逻辑或 (`OR`)

### 3. DQL 数据查询语句

#### 语法结构
```sql
[WITH cte_name [(column_name [, ...])] AS (SELECT ...)
     [, cte_name [(column_name [, ...])] AS (SELECT ...)]*]
SELECT select_item [, select_item]*
FROM table_name [AS alias]
     [[INNER] JOIN table_name [AS alias] ON column1 = column2]*
[WHERE condition [AND condition]*]
[GROUP BY column_name [, column_name]*]
[ORDER BY column_name]
[LIMIT number]
```

#### 子句说明

**WITH 子句（公共表表达式 CTE）**
- 支持定义一个或多个临时命名结果集
- 可选择性地指定列名列表
- 多个CTE用逗号分隔
- 示例：`WITH high_earners AS (SELECT * FROM employees WHERE salary > 10000)`

**SELECT 子句**
- `*` - 选择所有列
- `table.*` - 选择指定表的所有列
- `expression [AS alias]` - 支持表达式和列别名
- 支持函数调用表达式（聚合函数、单行函数）
- 示例：`SELECT id, name, salary * 1.1 AS new_salary`

**FROM 子句**
- 支持表别名：`FROM table_name AS alias`
- 支持多表连接（仅内连接）
- 示例：`FROM employees AS e`

**JOIN 子句**
- 仅支持 INNER JOIN（内连接）
- 连接条件必须是等值条件
- 支持多次连接
- 语法：`[INNER] JOIN table_name [AS alias] ON column1 = column2`
- 示例：`JOIN departments d ON e.dept_id = d.id`

**WHERE 子句**
- 支持列与字面量的比较
- 支持 AND 连接多个条件（最多2个条件）
- 比较运算符：`=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- 示例：`WHERE age > 25 AND salary > 5000`

**GROUP BY 子句**
- 支持单列或多列分组
- 多个列用逗号分隔
- 示例：`GROUP BY department, job_title`

**ORDER BY 子句**
- **仅支持单列排序**
- **仅支持升序排序（ASC，自然顺序）**
- 不支持 DESC（降序）
- 不支持多列排序
- 示例：`ORDER BY salary`

**LIMIT 子句**
- 限制返回的结果行数
- 接受整数字面量
- 示例：`LIMIT 10`

#### 完整示例
```sql
-- 基本查询
SELECT * FROM employees;

-- 带条件和排序
SELECT id, name, salary 
FROM employees 
WHERE salary > 5000 AND department = 'IT'
ORDER BY salary 
LIMIT 10;

-- 连接查询
SELECT e.name, d.department_name
FROM employees AS e
INNER JOIN departments AS d ON e.dept_id = d.id
WHERE e.salary > 8000;

-- 分组聚合
SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
FROM employees
GROUP BY department;

-- 使用CTE
WITH high_earners AS (
    SELECT * FROM employees WHERE salary > 10000
)
SELECT department, COUNT(*) 
FROM high_earners 
GROUP BY department;
```



### 4. 查询计划分析（EXPLAIN）

使用 `EXPLAIN` 关键字查看查询执行计划：

```sql
EXPLAIN SELECT * FROM employees WHERE salary > 5000;
```

**功能说明：**
- 显示查询的执行计划树
- 展示使用的操作符（扫描、连接、排序等）
- 帮助理解查询优化过程
- 用于性能分析和调优



### 5. DML 数据操作语句

#### 5.1 INSERT - 插入数据

**语法：**
```sql
INSERT INTO table_name VALUES (value1, value2, ...)[, (value1, value2, ...)]*;
```

**说明：**
- ✅ 支持单行插入
- ✅ 支持批量插入（多个 VALUES）
- ❌ 不支持指定列名（必须提供所有列的值）
- ❌ 不支持 INSERT ... SELECT 语法

**示例：**
```sql
-- 单行插入
INSERT INTO users VALUES (1, 'Alice', 25);

-- 批量插入
INSERT INTO users VALUES 
    (1, 'Alice', 25),
    (2, 'Bob', 30),
    (3, 'Charlie', 28);
```

#### 5.2 UPDATE - 更新数据

**语法：**
```sql
UPDATE table_name SET column_name = expression [WHERE expression];
```

**说明：**
- ✅ 支持基于表达式的更新
- ⚠️ 一次只能更新一个列
- ✅ WHERE 子句可选（省略则更新所有行）
- ✅ WHERE 条件支持复杂表达式

**示例：**
```sql
-- 更新单个字段
UPDATE employees SET salary = salary * 1.1 WHERE department = 'IT';

-- 更新所有行
UPDATE products SET status = 'active';
```

#### 5.3 DELETE - 删除数据

**语法：**
```sql
DELETE FROM table_name WHERE expression;
```

**说明：**
- ⚠️ WHERE 子句是必需的（防止误删全表）
- ✅ WHERE 条件支持复杂表达式

**示例：**
```sql
DELETE FROM users WHERE age < 18;
DELETE FROM orders WHERE status = 'cancelled' AND created_date < '2023-01-01';
```

### 6. DDL 数据定义语句

#### 6.1 CREATE TABLE - 创建表

**语法方式一：指定列定义**
```sql
CREATE TABLE table_name (
    column1 type1 [(size)],
    column2 type2 [(size)],
    ...
);
```

**语法方式二：从查询结果创建**
```sql
CREATE TABLE table_name AS SELECT ...;
```

**示例：**
```sql
-- 基本建表
CREATE TABLE employees (
    id INT,
    name STRING(50),
    salary FLOAT,
    active BOOL
);

-- 从查询结果创建表
CREATE TABLE high_earners AS 
    SELECT * FROM employees WHERE salary > 10000;
```

**限制：**
- ❌ 不支持约束（PRIMARY KEY、FOREIGN KEY、UNIQUE、CHECK、NOT NULL、DEFAULT）
- ❌ 不支持自增列
- ✅ STRING 和 BYTE_ARRAY 类型必须指定长度

#### 6.2 DROP TABLE - 删除表

**语法：**
```sql
DROP TABLE table_name;
```

**示例：**
```sql
DROP TABLE old_data;
```

#### 6.3 CREATE INDEX - 创建索引

**语法：**
```sql
CREATE INDEX ON table_name (column_name);
```

**示例：**
```sql
CREATE INDEX ON employees (department);
CREATE INDEX ON orders (customer_id);
```

**索引特性：**
- ✅ 使用 B+ 树数据结构
- ✅ 自动维护索引一致性
- ⚠️ 每个列最多创建一个索引
- ❌ 不支持复合索引（多列索引）
- ❌ 不支持唯一索引、全文索引等特殊类型

#### 6.4 DROP INDEX - 删除索引

**语法：**
```sql
DROP INDEX table_name (column_name);
```

**示例：**
```sql
DROP INDEX employees (department);
```

### 7. TCL 事务控制语句

RookieDB 实现了完整的 ACID 事务支持，使用可串行化隔离级别和 ARIES 恢复算法。

#### 7.1 BEGIN - 开始事务

**语法：**
```sql
BEGIN [TRANSACTION];
```

**示例：**
```sql
BEGIN;
BEGIN TRANSACTION;
```

#### 7.2 COMMIT - 提交事务

**语法：**
```sql
COMMIT [TRANSACTION];
END [TRANSACTION];
```

**示例：**
```sql
BEGIN;
INSERT INTO accounts VALUES (1, 1000);
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
COMMIT;
```

#### 7.3 ROLLBACK - 回滚事务

**完全回滚：**
```sql
ROLLBACK [TRANSACTION];
```

**回滚到保存点：**
```sql
ROLLBACK [TRANSACTION] TO [SAVEPOINT] savepoint_name;
```

**示例：**
```sql
-- 完全回滚
BEGIN;
DELETE FROM orders WHERE status = 'pending';
ROLLBACK;  -- 撤销删除操作

-- 部分回滚
BEGIN;
INSERT INTO logs VALUES (1, 'start');
SAVEPOINT sp1;
DELETE FROM logs WHERE id > 100;
ROLLBACK TO sp1;  -- 只撤销删除，保留插入
COMMIT;
```

#### 7.4 SAVEPOINT - 设置保存点

**语法：**
```sql
SAVEPOINT savepoint_name;
```

**说明：**
- 在事务中创建一个命名的保存点
- 允许部分回滚到该保存点
- 支持嵌套保存点

**示例：**
```sql
BEGIN;
INSERT INTO users VALUES (1, 'Alice');
SAVEPOINT after_insert;
UPDATE users SET name = 'Bob' WHERE id = 1;
ROLLBACK TO after_insert;  -- 撤销更新，保留插入
COMMIT;
```

#### 7.5 RELEASE - 释放保存点

**语法：**
```sql
RELEASE [SAVEPOINT] savepoint_name;
```

**说明：**
- 删除指定的保存点
- 释放后无法再回滚到该保存点
- 不影响事务中的数据修改

**示例：**
```sql
BEGIN;
SAVEPOINT sp1;
INSERT INTO data VALUES (1);
RELEASE sp1;  -- 释放保存点
-- 现在无法 ROLLBACK TO sp1
COMMIT;
```

**事务特性：**
- ✅ 可串行化隔离级别（Serializable）
- ✅ 严格两阶段锁协议（Strict 2PL）
- ✅ ARIES 恢复算法（支持崩溃恢复）
- ✅ 支持嵌套保存点
- ✅ 死锁检测和处理

### 8. 批量 SQL 语句执行

**语法：**
```sql
statement1; statement2; statement3; ...
```

**说明：**
- 使用分号 `;` 分隔多个 SQL 语句
- 按顺序依次执行
- 所有语句在同一个事务中执行（如果已开启事务）

**示例：**
```sql
CREATE TABLE users (id INT, name STRING(50));
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
```

---

## 📋 语法限制总结

### 不支持的 SQL 特性

**数据类型相关：**
- ❌ NULL 值
- ❌ DATE、TIME、TIMESTAMP 等时间类型
- ❌ DECIMAL、NUMERIC 等精确数值类型
- ❌ 可变长度字符串（VARCHAR 实际为固定长度）
- ❌ BLOB、CLOB 等大对象类型

**约束相关：**
- ❌ PRIMARY KEY（主键）
- ❌ FOREIGN KEY（外键）
- ❌ UNIQUE（唯一约束）
- ❌ CHECK（检查约束）
- ❌ NOT NULL（非空约束）
- ❌ DEFAULT（默认值）
- ❌ AUTO_INCREMENT（自增）

**查询相关：**
- ❌ 外连接（LEFT JOIN、RIGHT JOIN、FULL OUTER JOIN）
- ❌ 子查询（除了 CREATE TABLE AS SELECT）
- ❌ UNION、INTERSECT、EXCEPT 集合操作
- ❌ HAVING 子句
- ❌ DISTINCT 关键字
- ❌ ORDER BY 降序（DESC）
- ❌ ORDER BY 多列排序
- ❌ 窗口函数
- ❌ 递归查询

**DML 相关：**
- ❌ INSERT 指定列名
- ❌ INSERT ... SELECT
- ❌ UPDATE 多列
- ❌ MERGE/UPSERT

**其他：**
- ❌ 视图（VIEW）
- ❌ 存储过程和函数
- ❌ 触发器（TRIGGER）
- ❌ 用户权限管理
- ❌ 数据库和模式（DATABASE/SCHEMA）

## 💻 SQL 查询示例

### 预置测试数据
项目提供了三个示例 CSV 文件用于数据加载和查询测试，包含以下表结构：

**Student.csv** - 学生信息表
```sql
CREATE TABLE Students{
    cid INT,
    name STRING(20),
    major STRING(20),
    grade FLOAT
};
```

**Enrollments.csv** - 选课记录表  
```sql
CREATE TABLE Enrollments{
    sid INT,
    cid INT    
};
```

**Courses.csv** - 课程信息表
```sql
CREATE TABLE Courses{
    cid INT,
    name STRING(20),
    department STRING(20)
};
```

### 基础 DDL 操作
```sql
-- 创建表（注意：字符串类型使用 STRING 而非 VARCHAR）
CREATE TABLE Employee (
    id INT,
    name STRING(50),
    gpa FLOAT
);
```

```sql
-- 创建索引以优化查询性能
CREATE INDEX idx_gpa ON students(gpa);
```

### 数据操作 (DML)
```sql
-- 插入记录
INSERT INTO students VALUES (1, 'Alice', 'CS', 3.8);
INSERT INTO students VALUES (2, 'Bob', 'EE', 3.5);
```

```sql
-- 条件查询
SELECT * FROM students WHERE gpa > 3.6;
```

```sql
-- 批量插入
INSERT INTO students VALUES 
    (3, 'Charlie', 3.9),
    (4, 'Diana', 3.7);
```

### 事务管理
```sql
-- 事务控制示例
BEGIN TRANSACTION;
INSERT INTO students VALUES (5, 'Eve', 4.0);
UPDATE students SET gpa = 3.9 WHERE name = 'Bob';
COMMIT;  -- 或 ROLLBACK; 撤销更改
```

### 系统元命令
```sql
-- 查看所有表信息
\d;
```

```sql
-- 查看特定表结构
\d students;
```

```sql
-- 查看事务占有的锁
\lock;
```

## 📁 项目结构

```
sp25-rookiedb/
├── src/main/java/edu/berkeley/cs186/database/
│   ├── cli/              # 命令行界面和服务器
│   │   ├── parser/       # SQL解析器（JavaCC生成）
│   │   └── visitor/      # AST访问者模式实现
│   ├── concurrency/      # 并发控制（锁管理器）
│   ├── databox/          # 数据类型系统
│   ├── index/            # B+树索引实现
│   ├── io/               # 磁盘空间管理
│   ├── memory/           # 缓冲管理器
│   ├── query/            # 查询优化和执行
│   │   └── plan/         # 查询计划和算子
│   ├── recovery/         # ARIES恢复算法
│   └── table/            # 表和记录管理
├── src/test/             # 单元测试和集成测试
├── src/main/resources/   # 配置文件、示例表格(将被loadCSV加载)
├── docker-compose.yml    # Docker compose配置
├── Dockerfile            # Docker部署配置
├── pom.xml               # Maven构建配置
└── README.md             # 项目文档
```

## 🔧 技术实现

### 1. B+ 树索引
- 支持高效的点查询和范围查询
- 自动页面分裂和合并
- 支持批量加载优化

### 2. 查询优化器
- 基于成本的动态规划算法
- 左深连接树生成
- 支持多种连接算法选择（BNLJ, GHJ, SMJ）

### 3. 并发控制
- 多粒度锁（IS, IX, S, X, SIX）
- 严格两阶段锁协议（Strict 2PL）
- 死锁检测和处理

### 4. 恢复机制
- ARIES 恢复算法（Analysis, Redo, Undo）
- 写前日志（WAL）
- 检查点机制
- 模糊检查点支持

### 5. 执行引擎
- 火山模型（迭代器模型）
- 流水线执行
- 支持多种连接算法：
  - 块嵌套循环连接（BNLJ）
  - 优雅哈希连接（GHJ）
  - 排序归并连接（SMJ）

## 📊 性能特点

- **索引查询**: O(log n) 时间复杂度
- **缓冲管理**: 高效的页面缓存策略
- **并发支持**: 支持多客户端同时连接
- **事务吞吐**: 完整的 ACID 保证

## 🧪 测试

```bash
# 生成解析器代码（首次运行必须执行）
./parsergen.sh
# 或在 Windows 上
mvn clean compile

# 运行所有测试
mvn test

# 运行特定项目的测试
mvn test -Dproj=2  # 测试B+树索引
mvn test -Dproj=3  # 测试查询优化
mvn test -Dproj=4  # 测试并发控制
mvn test -Dproj=5  # 测试恢复管理

# 运行公开测试
mvn test -P public

# 运行系统测试
mvn test -P system

# 在 Docker 容器中运行测试
docker run --rm rookiedb mvn test
```

## 📚 参考资料

- [Database System Concepts](https://www.db-book.com/) - Silberschatz, Korth, Sudarshan
- [ARIES: A Transaction Recovery Method](https://dl.acm.org/doi/10.1145/128765.128770) - C. Mohan et al.
- [UC Berkeley CS186 Course](https://cs186berkeley.net/)

## 🤝 贡献

本项目基于 UC Berkeley CS186 课程项目开发，用于学习和研究目的。

## 📄 许可证

本项目仅供学习和研究使用。

## 🔍 故障排查

### Docker 相关问题

**错误：`cannot connect to Docker daemon`**
- 解决方法：启动 Docker Desktop 应用程序
- 验证：运行 `docker info` 确认 Docker 服务正常

**错误：端口被占用**
```bash
# 查找占用端口的进程
netstat -ano | findstr :18600  # Windows
lsof -i :18600                 # Linux/Mac

# 使用不同端口运行
docker run -d -p 18601:18600 --name rookiedb-server rookiedb
```

### Maven 构建问题

**错误：`Failed to execute goal ... Server`**
- 原因：JavaCC 解析器未生成
- 解决方法：
```bash
./parsergen.sh
mvn clean compile
mvn exec:java -Dexec.mainClass="edu.berkeley.cs186.database.cli.Server"
```

### 数据库连接问题

**无法连接到数据库服务器**
- 检查容器是否运行：`docker ps`
- 检查端口映射：`docker port rookiedb-server`
- 检查 Python 版本：`python --version`（需要 Python 3）

## ⚠️ 安全提示

本数据库服务器设计用于教学和开发环境，**不建议暴露到公共网络**：
- ❌ 没有内置的身份验证机制
- ❌ 没有加密传输
- ❌ 没有访问控制列表
- ✅ 仅适合在受信任的网络环境中使用
- ✅ 适合本地开发和学习

---

**开发者**: 基于 UC Berkeley CS186 Spring 2025 课程项目

**技术栈**: Java 11 · Maven · JavaCC · Docker
