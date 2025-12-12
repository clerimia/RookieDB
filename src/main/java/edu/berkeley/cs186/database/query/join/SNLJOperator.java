package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 使用简单嵌套循环连接算法对左右两个关系的leftColumnName和rightColumnName列执行等值连接。
 */
public class SNLJOperator extends JoinOperator {
    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        // 需要对右表先进行物化
        super(leftSource, materialize(rightSource, transaction),
              leftColumnName, rightColumnName, transaction, JoinType.SNLJ);
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        // 获取一个迭代器
        return new SNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        int numLeftRecords = getLeftSource().estimateStats().getNumRecords();
        int numRightPages = getRightSource().estimateStats().getNumPages();
        return numLeftRecords * numRightPages + getLeftSource().estimateIOCost();
    }

    /**
     * 执行简单嵌套循环连接逻辑的记录迭代器。
     * 注意左表是"外"循环，右表是"内"循环。
     */
    private class SNLJIterator implements Iterator<Record> {
        // 左关系所有记录的迭代器
        private Iterator<Record> leftSourceIterator;
        // 右关系所有记录的迭代器
        private BacktrackingIterator<Record> rightSourceIterator;
        // 来自左关系的当前记录
        private Record leftRecord;
        // 下一个要返回的记录
        private Record nextRecord;

        public SNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();

            // 先缓存一个左边的记录
            if (leftSourceIterator.hasNext()) leftRecord = leftSourceIterator.next();

            // 设置第一个记录为返回点
            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
        }

        /**
         * 返回从此连接中应该产生的下一个记录，
         * 如果没有更多记录可连接则返回null。
         */
        private Record fetchNextRecord() {
            if (leftRecord == null) {
                // 左源为空，无数据可获取
                return null;
            }

            while(true) {
                if (this.rightSourceIterator.hasNext()) {
                    // 存在下一个右记录，如果有匹配则进行连接
                    Record rightRecord = rightSourceIterator.next();

                    // 进行匹配
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftSourceIterator.hasNext()){
                    // 没有更多的右记录但仍有左记录。
                    // 推进左记录并重置右迭代器
                    this.leftRecord = leftSourceIterator.next();
                    this.rightSourceIterator.reset();
                } else {
                    // 如果执行到这里说明没有更多记录可获取
                    return null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }

}

