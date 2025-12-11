# BufferManager 组件 API 文档

## 概述

BufferManager 是数据库系统中的缓冲区管理器，负责管理内存中的页面缓存。它实现了页面置换策略，协调磁盘和内存之间的数据传输，并维护页面的一致性和事务日志记录。

## 主要职责

1. **页面缓存管理**：在内存中缓存磁盘页面，减少磁盘I/O操作
2. **页面置换策略**：使用可配置的置换策略（LRU、Clock等）决定哪些页面应该被驱逐
3. **页面生命周期管理**：处理页面的加载、卸载、刷新和释放
4. **并发控制**：通过锁机制保证多线程环境下的页面访问安全
5. **恢复支持**：与恢复管理器协作，记录页面更改的日志信息

## 核心类

### BufferManager 类

#### 构造函数

```
public BufferManager(DiskSpaceManager diskSpaceManager, RecoveryManager recoveryManager, 
                     int bufferSize, EvictionPolicy evictionPolicy)
```

- **功能**: 创建一个新的缓冲区管理器实例
- **参数**:
  - `diskSpaceManager`: 底层磁盘空间管理器
  - `recoveryManager`: 恢复管理器
  - `bufferSize`: 缓冲区大小（以页为单位）
  - `evictionPolicy`: 页面置换策略

#### 主要公共方法

##### 获取页面

```
public Page fetchPage(LockContext parentContext, long pageNum)
```

- **功能**: 获取指定页面，返回已加载并锁定的缓冲区帧包装的页面对象
- **参数**:
  - `parentContext`: 页面父级的锁上下文
  - `pageNum`: 页面编号
- **返回**: 指定的页面

##### 获取新页面

```
public Page fetchNewPage(LockContext parentContext, int partNum)
```

- **功能**: 获取一个新页面，返回已加载并锁定的缓冲区帧包装的新页面对象
- **参数**:
  - `parentContext`: 新页面的父级锁上下文
  - `partNum`: 新页面的分区号
- **返回**: 新页面

##### 释放页面

```
public void freePage(Page page)
```

- **功能**: 释放一个页面 - 从缓存中驱逐该页面，并通知磁盘空间管理器该页面不再需要
- **参数**:
  - `page`: 要释放的页面

##### 释放分区

```
public void freePart(int partNum)
```

- **功能**: 释放一个分区 - 从缓存中驱逐所有相关页面，并通知磁盘空间管理器该分区不再需要
- **参数**:
  - `partNum`: 要释放的分区号

##### 驱逐页面

```
public void evict(long pageNum)
```

- **功能**: 调用页面帧的flush方法并将页面从帧中卸载
- **参数**:
  - `pageNum`: 要驱逐的页面号

##### 驱逐所有页面

```
public void evictAll()
```

- **功能**: 按顺序对每个帧调用evict方法

##### 遍历页面号

```
public void iterPageNums(BiConsumer<Long, Boolean> process)
```

- **功能**: 对每个已加载页面的页面号调用传入的方法
- **参数**:
  - `process`: 处理页面号的方法。第一个参数是页面号，第二个参数是表示页面是否脏（有未刷新更改）的布尔值

##### 获取I/O数量

```
public long getNumIOs()
```

- **功能**: 获取自缓冲区管理器启动以来的I/O操作数（不包括磁盘空间管理中使用的任何内容）
- **返回**: I/O操作数

### Frame 内部类

Frame 类代表缓冲区中的单个页面帧，继承自 BufferFrame。它封装了页面的内容以及相关的元数据。

#### 主要方法

##### 锁定帧

```
@Override
public void pin()
```

- **功能**: 锁定缓冲区帧；在锁定期间不能被驱逐。当缓冲区帧被锁定时会发生"命中"。

##### 解锁帧

```
@Override
public void unpin()
```

- **功能**: 解锁缓冲区帧。

##### 检查有效性

```
@Override
public boolean isValid()
```

- **功能**: 检查此帧是否有效
- **返回**: 如果帧有效返回true

##### 获取页面号

```
@Override
public long getPageNum()
```

- **功能**: 获取此帧的页面号
- **返回**: 页面号

##### 刷新到磁盘

```
@Override
void flush()
```

- **功能**: 将此缓冲区帧刷新到磁盘，但不卸载它。

##### 读取字节

```
@Override
void readBytes(short position, short num, byte[] buf)
```

- **功能**: 从缓冲区帧读取数据
- **参数**:
  - `position`: 开始读取的位置
  - `num`: 要读取的字节数
  - `buf`: 输出缓冲区

##### 写入字节

```
@Override
void writeBytes(short position, short num, byte[] buf)
```

- **功能**: 写入数据到缓冲区帧，并标记帧为脏
- **参数**:
  - `position`: 开始写入的位置
  - `num`: 要写入的字节数
  - `buf`: 输入缓冲区

## 常量

- `RESERVED_SPACE`: 为恢复簿记保留的字节数（36字节）
- `EFFECTIVE_PAGE_SIZE`: 缓冲区管理器用户可用的有效页面大小

## 设计特点

1. **页面替换策略可插拔**：支持不同的页面替换算法（LRU、Clock等）
2. **线程安全**：使用锁机制保证并发访问的安全性
3. **恢复支持**：与ARIES恢复协议集成，记录页面修改日志
4. **高效内存管理**：通过帧复用减少内存分配开销