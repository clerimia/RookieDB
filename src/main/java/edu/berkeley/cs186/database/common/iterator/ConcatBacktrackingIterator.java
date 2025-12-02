package edu.berkeley.cs186.database.common.iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * 将一组支持回溯的可迭代对象连接起来的迭代器。
 * 例如，如果你有包含以下内容的回溯迭代器：
 * - [1,2,3]
 * - []
 * - [4,5,6]
 * - [7,8]
 *
 * 使用此类连接它们会产生一个值为[1,2,3,4,5,6,7,8]的回溯迭代器。
 */
public class ConcatBacktrackingIterator<T> implements BacktrackingIterator<T> {
    // 我们正在连接的可迭代对象的迭代器
    private BacktrackingIterator<BacktrackingIterable<T>> outerIterator;
    // 我们正在连接的可迭代对象列表
    private List<BacktrackingIterable<T>> iterables;
    // 产生上一个项目的迭代器
    private BacktrackingIterator<T> prevItemIterator;
    // 我们将从中产生下一个项目的迭代器
    private BacktrackingIterator<T> nextItemIterator;
    // 包含当前标记项目的迭代器
    private BacktrackingIterator<T> markItemIterator;
    // 上述三个迭代器源可迭代对象的索引
    private int prevIndex = -1;
    private int nextIndex = -1;
    private int markIndex = -1;

    /**
     * @param outerIterator 一个迭代器，用于遍历我们想要连接的可迭代对象。
     *                      此迭代器产生的任何值都将来自从outerIterator内容创建的迭代器。
     */
    public ConcatBacktrackingIterator(BacktrackingIterator<BacktrackingIterable<T>> outerIterator) {
        this.iterables = new ArrayList<>();
        this.outerIterator = outerIterator;
        this.prevItemIterator = null;
        this.nextItemIterator = new EmptyBacktrackingIterator<>();
        this.markItemIterator = null;
    }

    /**
     * 将nextItemIterator设置为我们集合中的下一个非空迭代器，如果所有剩余的迭代器都为空，则设置为最后一个迭代器。
     * 根据需要从outerIterator惰性地添加可迭代对象到可迭代对象列表中。
     */
    private void moveNextToNonEmpty() {
        while (!this.nextItemIterator.hasNext()) {
            if (nextIndex + 1 < iterables.size()) {
                nextIndex++;
                this.nextItemIterator = iterables.get(nextIndex).iterator();
            } else {
                assert(nextIndex + 1 == iterables.size());
                if (!outerIterator.hasNext()) break;
                iterables.add(outerIterator.next());
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (!this.nextItemIterator.hasNext()) this.moveNextToNonEmpty();
        return this.nextItemIterator.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T item = this.nextItemIterator.next();
        this.prevItemIterator = this.nextItemIterator;
        prevIndex = nextIndex;
        return item;
    }

    @Override
    public void markPrev() {
        if (prevIndex == -1) {
            // 我们还没有产生项目，或者自从最近一次重置以来没有产生项目
            return;
        }
        this.markItemIterator = this.prevItemIterator;
        this.markItemIterator.markPrev();
        markIndex = prevIndex;
    }

    @Override
    public void markNext() {
        if (!hasNext()) return;
        this.markItemIterator = this.nextItemIterator;
        this.markItemIterator.markNext();
        markIndex = nextIndex;
    }

    @Override
    public void reset() {
        if (markIndex == -1) {
            // 没有标记任何内容
            return;
        }
        // 我们不再有前一个项目
        prevItemIterator = null;
        prevIndex = -1;

        // 将我们的下一个设置为标记的位置
        this.nextItemIterator = this.markItemIterator;
        this.nextItemIterator.reset();
        nextIndex = markIndex;
    }
}