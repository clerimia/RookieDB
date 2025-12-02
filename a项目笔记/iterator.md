## 回溯迭代器的API
`markPrev` 标记上一个  
`markNext` 标记下一个  
`reset`    返回  

## Index回溯迭代器
专门为可以通过索引访问的数据结构设计  
### 核心字段，状态
- maxIndex 集合的最大索引值
- prevIndex 上一个要返回元素的索引
- nextIndex 下一个要返回元素的索引
- markIndex 标记位置的索引

### 核心API
#### 构造函数
需要输入最大索引值
```java
public IndexBacktrackingIterator(int maxIndex) {
        this.maxIndex = maxIndex;
}
```

#### 抽象方法：需要子类实现
- 获取下一个非空值的索引  getNextNonEmpty(currentIndex)
- 获取指定索引的值       getValue(index)

#### 模版方法：已经实现
- hasNext: 首次调用会调用getNextNonEmpty 获取下一个索引值，然后判断 nextIndex <? max Index
```java
    @Override
    public boolean hasNext() {
        if (this.nextIndex == -1) this.nextIndex = getNextNonEmpty(nextIndex);
        return this.nextIndex < this.maxIndex;
    }
```

- next(): 获取下一个元素, 会记录状态。 牢记：prevIndex, nextIndex, markIndex, maxIndex
```java
    @Override
    public T next() {
        if (!this.hasNext()) throw new NoSuchElementException();
        T value = getValue(this.nextIndex);
        this.prevIndex = this.nextIndex;
        this.nextIndex = this.getNextNonEmpty(this.nextIndex);
        return value;
    }
```

- markPrev() 标记上一个元素位置。 即：markIndex = prevIndex, 如果没有prevIndex 就直接返回
```java
    @Override
    public void markPrev() {
        if (prevIndex == -1) return;
        this.markIndex = this.prevIndex;
    }
```

- markNext() 标记下一个元素位置。 即：markIndex = nextIndex
```java
    @Override
    public void markNext() {
        if (hasNext()) markIndex = nextIndex;
    }
```


- reset 重置，即nextIndex = markIndex
```java
    @Override
    public void reset() {
        if (this.markIndex == -1) return;
        this.prevIndex = -1;
        this.nextIndex = this.markIndex;
    }
```



## 几种不同的迭代器
### 数组回溯迭代器

比较简单，维护的状态有
- 内部数组

### 合并迭代器
估计是用于SortMerge产生的结果，会产生一个回溯迭代器


### 空迭代器
- hasNext 只返回false
- next 直接报错