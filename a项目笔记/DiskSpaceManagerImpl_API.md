# DiskSpaceManagerImpl 组件 API 文档

## 概述

DiskSpaceManagerImpl 是数据库系统的磁盘空间管理器实现，负责管理磁盘上的数据页面存储。它采用虚拟页面转换机制，并具有两级页眉页面结构，允许每个分区存储大量数据（在4K页面大小的情况下，每个分区可达256GB）。

## 主要职责

1. **磁盘空间管理**：管理磁盘上数据页面的分配和释放
2. **虚拟页面转换**：实现虚拟页面号码到物理页面位置的映射
3. **分区管理**：管理多个分区，每个分区对应一个操作系统级别的文件
4. **页面分配跟踪**：通过位图跟踪已分配和未分配的数据页面
5. **持久化存储**：确保主页面和页眉页面的更改立即刷新到磁盘

## 核心类

### DiskSpaceManagerImpl 类

#### 构造函数

```
public DiskSpaceManagerImpl(String dbDir, RecoveryManager recoveryManager)
```

- **功能**: 使用给定目录初始化磁盘空间管理器。如果目录不存在则创建该目录。
- **参数**:
  - `dbDir`: 数据库的基本目录路径
  - `recoveryManager`: 恢复管理器

#### 主要公共方法

##### 分配新分区

```
@Override
public int allocPart()
```

- **功能**: 分配一个新的分区，返回分区号。
- **返回**: 新分配的分区号

##### 分配指定分区号的分区

```
@Override
public int allocPart(int partNum)
```

- **功能**: 分配一个指定分区号的分区。
- **参数**:
  - `partNum`: 指定的分区号
- **返回**: 分区号

##### 释放分区

```
@Override
public void freePart(int partNum)
```

- **功能**: 释放指定的分区。
- **参数**:
  - `partNum`: 要释放的分区号

##### 分配新页面

```
@Override
public long allocPage(int partNum)
```

- **功能**: 在指定分区中分配一个新页面。
- **参数**:
  - `partNum`: 分区号
- **返回**: 分配的虚拟页面号

##### 分配指定页面号的页面

```
@Override
public long allocPage(long page)
```

- **功能**: 分配一个指定页面号的页面。
- **参数**:
  - `page`: 虚拟页面号
- **返回**: 分配的虚拟页面号

##### 释放页面

```
@Override
public void freePage(long page)
```

- **功能**: 释放指定的页面。
- **参数**:
  - `page`: 虚拟页面号

##### 读取页面

```
@Override
public void readPage(long page, byte[] buf)
```

- **功能**: 读取指定页面的内容到缓冲区。
- **参数**:
  - `page`: 虚拟页面号
  - `buf`: 用于存储页面内容的缓冲区（必须是页面大小）

##### 写入页面

```
@Override
public void writePage(long page, byte[] buf)
```

- **功能**: 将缓冲区内容写入指定页面。
- **参数**:
  - `page`: 虚拟页面号
  - `buf`: 包含要写入数据的缓冲区（必须是页面大小）

##### 检查页面是否已分配

```
@Override
public boolean pageAllocated(long page)
```

- **功能**: 检查指定页面是否已经分配。
- **参数**:
  - `page`: 虚拟页面号
- **返回**: 如果页面已分配返回true，否则返回false

##### 关闭磁盘空间管理器

```
@Override
public void close()
```

- **功能**: 关闭磁盘空间管理器，释放所有资源。

## 常量

- `MAX_HEADER_PAGES`: 每个页眉页面的最大数量 (PAGE_SIZE / 2)
- `DATA_PAGES_PER_HEADER`: 每个页眉页面管理的数据页面数量 (PAGE_SIZE * 8)

## 设计特点

1. **两级页眉结构**：
   - 主页面(Master Page)存储每个页眉页面的分配计数
   - 页眉页面(Header Page)存储数据页面分配的位图

2. **虚拟页面号码格式**：
   - 格式为：分区号 * 10^10 + n（第n个数据页面）
   - 这种格式便于调试和识别页面归属

3. **文件组织结构**：
   - 主页面是OS文件的第0页
   - 第一个页眉页面是OS文件的第1页
   - 后续是数据页面和其他页眉页面

4. **内存缓存**：
   - 主页面和页眉页面永久缓存在内存中
   - 对这些页面的更改会立即刷新到磁盘

5. **并发控制**：
   - 使用锁机制保证多线程环境下的安全性
   - 每个分区有自己的锁

## 页面号码转换

虚拟页面号码采用64位整数(long)格式：
```
分区号 * 10^10 + n
```
其中n是该分区的第n个数据页面（从0开始索引）。