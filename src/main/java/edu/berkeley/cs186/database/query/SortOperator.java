package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * 返回一个包含输入迭代器中记录的已排序Run。
     * 您可以自由地使用Java内置排序方法之一对所有记录进行内存内排序。
     *
     * @return 包含来自输入迭代器的所有记录的单个已排序Run
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        return null;
    }

    /**
     * 给定一个已排序的Run列表，返回一个新的Run，它是合并输入Run的结果。
     * 您应该使用优先队列(java.util.PriorityQueue)来确定下一个应该添加到输出Run中的记录。
     *
     * 在任何给定时刻，您不允许在优先队列中有超过runs.size()条记录。
     * 建议您的优先队列保存Pair<Record, Integer>对象，
     * 其中Pair (r, i)表示当前从未合并的第i个Run中取出的具有最小排序值的记录r。
     * `i`在从队列中移除最小元素后，可用于定位下一个要添加到队列中的记录。
     *
     * @return 通过合并输入Run获得的单个已排序Run
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        return null;
    }

    /**
     * 仅根据默认比较器比较两个(记录, 整数)对的记录组件。
     * 您可能会发现这对实现mergeSortedRuns很有用。
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * 给定N个已排序的Run列表，返回一个已排序Run列表，
     * 该列表是每次合并(numBuffers - 1)个输入Run的结果。
     * 如果N不是(numBuffers - 1)的完美倍数，则最后一个已排序Run应该是合并少于(numBuffers - 1)个Run的结果。
     *
     * @return 通过合并输入Run获得的已排序Run列表
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        return Collections.emptyList();
    }

    /**
     * 对源操作符的记录执行外部归并排序。
     * 您可能会发现QueryOperator类的getBlockIterator方法在这里很有用，
     * 可以用来创建初始的已排序Run集合。
     *
     * @return 包含源操作符所有记录的单个已排序Run
     */
    public Run sort() {
        // 我们想要排序的关系的记录迭代器
        Iterator<Record> sourceIterator = getSource().iterator();

        // TODO(proj3_part1): implement
        return makeRun(); // TODO(proj3_part1): replace this!
    }

    /**
     * @return 一个新的空Run。
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return 包含`records`中记录的新Run
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

