package edu.berkeley.cs186.database.common.iterator;

import java.util.NoSuchElementException;

/**
 * 空的回溯迭代器。在 markPrev()、markNext() 和 reset() 上不执行任何操作。
 * hasNext() 总是返回 false。
 */
public class EmptyBacktrackingIterator<T> implements BacktrackingIterator<T> {
    @Override public boolean hasNext() { return false; }
    @Override public T next() { throw new NoSuchElementException(); }
    @Override public void markPrev() {}
    @Override public void markNext() {}
    @Override public void reset() {}
}
