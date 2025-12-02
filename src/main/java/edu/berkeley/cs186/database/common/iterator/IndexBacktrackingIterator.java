package edu.berkeley.cs186.database.common.iterator;

import java.util.NoSuchElementException;

/**
 * 对支持索引的集合的部分回溯迭代器实现，
 * 其中某些索引可能是"空"的，不对应任何值。
 * 子类只需要实现getNextNonEmpty(int)和getValue(int)方法，
 * 标记和重置逻辑的其余部分在此类中处理。
 */
public abstract class IndexBacktrackingIterator<T> implements BacktrackingIterator<T> {
    private int maxIndex; // 集合的最高索引
    private int prevIndex = -1; // 上一个产生的项目的索引
    private int nextIndex = -1; // 下一个要产生的项目的索引
    private int markIndex = -1; // 最近标记的项目的索引

    public IndexBacktrackingIterator(int maxIndex) {
        this.maxIndex = maxIndex;
    }

    /**
     * 获取下一个非空索引。初始调用使用-1。
     * @return 下一个非空索引，如果没有更多值则返回最大索引。
     */
    protected abstract int getNextNonEmpty(int currentIndex);

    /**
     * 获取给定索引处的值。索引始终是getNextNonEmpty返回的值。
     * @param index 要获取值的索引
     * @return 索引处的值
     */
    protected abstract T getValue(int index);

    @Override
    public boolean hasNext() {
        if (this.nextIndex == -1) this.nextIndex = getNextNonEmpty(nextIndex);
        return this.nextIndex < this.maxIndex;
    }

    @Override
    public T next() {
        if (!this.hasNext()) throw new NoSuchElementException();
        T value = getValue(this.nextIndex);
        this.prevIndex = this.nextIndex;
        this.nextIndex = this.getNextNonEmpty(this.nextIndex);
        return value;
    }

    @Override
    public void markPrev() {
        if (prevIndex == -1) return;
        this.markIndex = this.prevIndex;
    }

    @Override
    public void markNext() {
        if (hasNext()) markIndex = nextIndex;
    }

    @Override
    public void reset() {
        if (this.markIndex == -1) return;
        this.prevIndex = -1;
        this.nextIndex = this.markIndex;
    }
}
