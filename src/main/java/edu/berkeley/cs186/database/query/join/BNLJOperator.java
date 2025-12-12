package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 使用块嵌套循环连接算法对两个关系的leftColumnName和rightColumnName列执行等值连接。
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        // 缓冲池个数
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //该方法实现了块嵌套循环连接的IO成本估算
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * 执行简单嵌套循环连接逻辑的记录迭代器。
     * 如果想了解fetchNextRecord()逻辑，可以查看SNLJOperator中的实现。
     */
    private class BNLJIterator implements Iterator<Record>{
        // 左源所有记录的迭代器
        private Iterator<Record> leftSourceIterator;
        // 右源所有记录的迭代器
        private BacktrackingIterator<Record> rightSourceIterator;
        // 当前左页块中记录的迭代器
        private BacktrackingIterator<Record> leftBlockIterator;
        // 当前右页中记录的迭代器
        private BacktrackingIterator<Record> rightPageIterator;
        // 左关系中的当前记录
        private Record leftRecord;
        // 下一个要返回的记录
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            // 获取下一个左表的块
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            // 获取下一个右表的页
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * 从左源获取下一个记录块。
         * leftBlockIterator应该设置为左源最多B-2页记录的回溯迭代器，
         * leftRecord应该设置为此块中的第一条记录。
         *
         * 如果左源中没有更多记录，则此方法不应执行任何操作。
         *
         * 您可能会发现QueryOperator#getBlockIterator在此处很有用。
         * 确保向此方法传递正确的模式。
         */
        private void fetchNextLeftBlock() {
            if (!leftSourceIterator.hasNext()) return;

            // 获取块的回溯迭代器
            BacktrackingIterator<Record> blockIterator = QueryOperator.getBlockIterator(
                    leftSourceIterator,             // 左源迭代器
                    getLeftSource().getSchema(),    // 左源的架构
                    numBuffers - 2                  // 缓冲池个数
            );

            // 设置leftRecord 和 leftBlockIterator
            blockIterator.markNext();
            this.leftRecord = blockIterator.next();
            this.leftBlockIterator = blockIterator;
        }

        /**
         * 从右源获取下一页记录。
         * rightPageIterator应该设置为右源最多一页记录的回溯迭代器。
         *
         * 如果右源中没有更多记录，则此方法不应执行任何操作。
         *
         * 您可能会发现QueryOperator#getBlockIterator在此处很有用。
         * 确保向此方法传递正确的模式。
         */
        private void fetchNextRightPage() {
            // 如果右源中没有更多记录，直接返回
            if (!rightSourceIterator.hasNext()) return;
            BacktrackingIterator<Record> blockIterator = QueryOperator.getBlockIterator(
                    rightSourceIterator,                // 右源迭代器
                    getRightSource().getSchema(),       // 右源架构；
                    1
            );

            // 设置回溯点
            blockIterator.markNext();
            this.rightPageIterator = blockIterator;
        }

        /**
         * 返回从此连接中应该产生的下一条记录，
         * 如果没有更多记录可连接则返回null。
         *
         * 您可能会发现JoinOperator#compare在此处很有用。
         * （您可以直接从本文件调用compare函数，因为BNLJOperator是JoinOperator的子类）。
         */
        private Record fetchNextRecord() {
            // 左源为空，无数据可获取
            if (leftRecord == null) {
                return null;
            }

            while (true) {
                if (rightPageIterator.hasNext()) {
                    // case 1 右迭代器有值
                    Record rightRecord = rightPageIterator.next();
                    // 比较是否满足连接条件，满足就拼接
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }

                } else if (leftBlockIterator.hasNext()) {
                    // case 2 右页迭代器没有值了，但是左块有
                    // 推进左记录并重置右迭代器
                    leftRecord = leftBlockIterator.next();
                    rightPageIterator.reset();

                } else if (rightSourceIterator.hasNext()) {
                    // case 3 右页和左块迭代器都没有值可以生成，但还有更多的右页
                    // 左块迭代器重置，加载新的leftRecord，加载新的右页
                    leftBlockIterator.reset();
                    leftRecord = leftBlockIterator.next();
                    fetchNextRightPage();

                } else if (leftSourceIterator.hasNext()) {
                    // case 4 右页和左块迭代器都没有值，也没有更多的右页，但还有左块
                    // 加载新的左块，回溯右源迭代器，加载新的右页
                    fetchNextLeftBlock();
                    rightSourceIterator.reset();
                    fetchNextRightPage();
                } else {
                    // 到这里就是join完成了，没有更多记录生成了
                    return null;
                }
            }
        }

        /**
         * @return 如果此迭代器还有另一条记录要产生则返回true，否则返回false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return 此迭代器的下一条记录
         * @throws NoSuchElementException 如果没有更多记录可产生
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
