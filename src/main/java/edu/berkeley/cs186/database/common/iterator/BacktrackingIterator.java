package edu.berkeley.cs186.database.common.iterator;

import java.util.Iterator;

/**
 * BacktrackingIterator支持在迭代过程中标记一个点，并将迭代器的状态重置回该标记点。
 * 这个是用于实现SortMergeJoin的迭代器
 * 例如，如果你有一个值为[1,2,3]的回溯迭代器：
 *
 * BackTrackingIterator<Integer> iter = new BackTrackingIteratorImplementation();
 * iter.next();     // 返回 1
 * iter.next();     // 返回 2
 * iter.markPrev(); // 标记先前返回的值，即 2
 * iter.next();     // 返回 3
 * iter.hasNext();  // 返回 false
 * iter.reset();    // 重置到标记的值（第5行）
 * iter.hasNext();  // 返回 true
 * iter.next();     // 返回 2
 * iter.markNext(); // 标记下一个要返回的值，即 3
 * iter.next();     // 返回 3
 * iter.hasNext();  // 返回 false
 * iter.reset();    // 重置到标记的值（第11行）
 * iter.hasNext();  // 返回 true
 * iter.next();     // 返回 3
 */
public interface BacktrackingIterator<T> extends Iterator<T> {
    /**
     * markPrev() 标记迭代器的最后一个返回值，也就是 next() 的最后返回值。
     *
     * 对于尚未产生任何记录的迭代器，或自上次 reset() 调用以来未产生记录的迭代器，
     * 调用 markPrev() 不执行任何操作。
     */
    void markPrev();

    /**
     * markNext() 标记迭代器的下一个返回值，即下一次调用 next() 所返回的值。
     *
     * 对于没有剩余记录的迭代器，调用 markNext() 不执行任何操作。
     */
    void markNext();

    /**
     * reset() 将迭代器重置到最后标记的位置。下一次调用 next() 应该返回被标记的值。
     * 如果没有任何标记，则 reset() 不执行任何操作。在设置新标记之前，可以根据需要
     * 多次重置到同一点。
     */
    void reset();
}

