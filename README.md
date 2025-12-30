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
