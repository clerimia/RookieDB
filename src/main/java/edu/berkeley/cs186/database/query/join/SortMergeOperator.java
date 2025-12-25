package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(
                prepareLeft(transaction, leftSource, leftColumnName),       // 预处理左边的操作符，让他有序
                prepareRight(transaction, rightSource, rightColumnName),    // 预处理右边的操作符，让他有序
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.SORTMERGE
        );
        this.stats = this.estimateStats();  // 获取统计信息
    }

    /**
     * 如果左源已经在目标列上排序，则返回leftSource，否则将左源包装在排序操作符中。
     */
    private static QueryOperator prepareLeft(
            TransactionContext transaction,
            QueryOperator leftSource,
            String leftColumn
    ) {
        // 获取连接字段名
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        // 看左源操作符是否在该字段上有序，如果有序直接返回
        if (leftSource.sortedBy().contains(leftColumn)) {
            return leftSource;
        }
        // 如果无序，包装一个排序操作符
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * 如果右源未排序，则将其包装在排序操作符中。
     * 否则，如果它未物化，则将其包装在物化操作符中。
     * 否则，直接返回右源。注意右源必须被物化，因为我们可能需要回溯它，而左源不需要。
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        // 获取连接字段
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);

        // 看右源操作符是否已排序，如果未排序，就包装一层排序操作符
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            // 如果排序了，但是没有物化，也就是无法回溯，就包装一层物化操作符
            return new MaterializeOperator(rightSource, transaction);
        }
        // 如果排序且物化，就直接返回
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //什么都不做
        return 0;
    }

    /**
     * Iterator的一个实现，为该操作符提供迭代器接口。
     *    参见课件幻灯片。
     *
     * 在继续之前，你应该阅读并理解SNLJOperator.java
     *    你可以在与本文件相同的目录中找到它。
     *
     * 建议：尝试将问题分解为可区分的子问题。
     *    这意味着你可能需要添加比给定更多的方法（再次提醒，
     *    SNLJOperator.java可能是一个有用的参考）。
     *
     */
    private class SortMergeIterator implements Iterator<Record> {
        /**
        * 提供了一些成员变量作为指导，但有很多可能的解决方案。
        * 你应该实现最适合你的解决方案，使用任何你需要的成员变量。
        * 你可以自由使用这些成员变量，但不是必须使用。
        */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        /**
         * 左表的迭代器一直是往前走的，
         * 但是右表的迭代器需要回溯，所以需要物化
         * */
        private SortMergeIterator() {
            super();
            // 左源迭代器
            leftIterator = getLeftSource().iterator();
            // 右源回溯迭代器
            rightIterator = getRightSource().backtrackingIterator();
            // 标记回溯点
            rightIterator.markNext();

            // 获取第一个记录
            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            // 默认没有标记
            this.marked = false;
        }

        /**
         * @return 如果此迭代器还有另一个要产生的记录，则返回true，否则返回false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return 此迭代器的下一个记录
         * @throws NoSuchElementException 如果没有更多记录可产生
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }

        /**
         * 返回应该从此连接中产生的下一条记录，
         * 如果没有更多记录要连接，则返回null。
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): 实现
            while (leftRecord != null && rightRecord != null) {
                int compareResult = compare(leftRecord, rightRecord);

                // 还没有到匹配段
                if (!marked) {
                    // 寻找匹配段的开始位置
                    if (compareResult < 0) {
                        // 左记录小于右记录，移动左迭代器
                        if (leftIterator.hasNext()) {
                            leftRecord = leftIterator.next();
                        } else {
                            leftRecord = null;
                        }
                    } else if (compareResult > 0) {
                        // 左记录大于右记录，移动右迭代器
                        if (rightIterator.hasNext()) {
                            rightRecord = rightIterator.next();
                        } else {
                            rightRecord = null;
                        }
                    } else {
                        // 找到匹配段，标记当前位置
                        marked = true;
                        rightIterator.markPrev();
                    }
                } else {
                    // 处理匹配段
                    if (compareResult == 0) {
                        // 记录匹配，创建连接结果
                        Record joinRecord = leftRecord.concat(rightRecord);

                        // 移动右迭代器
                        if (rightIterator.hasNext()) {
                            rightRecord = rightIterator.next();
                        } else {
                            // 右迭代器到达末尾，重新标记并移动左迭代器
                            remark();
                        }

                        return joinRecord;
                    } else {
                        // 当前匹配段结束，重新标记并继续
                        remark();
                    }
                }
            }

            return null;
        }

        /**
         * 回溯右迭代器并尝试推进左迭代器
         * 当右表遍历完一轮或当前左右记录不匹配时调用此方法
         */
        private void remark() {
            // 重置右迭代器到标记位置（相同值段的开始）
            rightIterator.reset();
            // 获取右表当前值段的第一条记录
            rightRecord = rightIterator.next();

            // 尝试推进左迭代器到下一条记录
            if (leftIterator.hasNext()) {
                // 获取左表的下一条记录并清除标记状态
                leftRecord = leftIterator.next();
                marked = false;
            } else {
                // 左表已遍历完毕，整个连接操作结束
                leftRecord = null;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
