package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.disk.Partition;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class SHJOperator extends JoinOperator {
    private int numBuffers;     // 缓冲区个数
    private Run joinedRecords;  // 连接记录，被物化到一个临时表中

    /**
     * 这个类表示一个简单的哈希连接。为了连接两个关系，
     * 该类将尝试对左记录进行一次分区阶段，然后用所有右记录进行探测。
     * 如果任何分区大于构建内存哈希表所需的B-2页内存，它将通过抛出IllegalArgumentException来失败。
     */
    public SHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // 由于这有可能在某些输入上失败，我们给它最大可能的成本以让优化器避免使用它
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // 在这个运行中累积我们所有的连接记录，一旦算法完成就返回它的迭代器
            this.joinedRecords = new Run(getTransaction(), getSchema());
            this.run(getLeftSource(), getRightSource(), 1);
        };
        return joinedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * 分区阶段。对于左记录迭代器中的每个记录，哈希我们要连接的值并将该记录添加到正确的分区。
     */
    private void partition(Partition[] partitions, Iterable<Record> leftRecords) {
        for (Record record: leftRecords) {
            // 根据所选列对左记录进行分区
            DataBox columnValue = record.getValue(getLeftColumnIndex());
            int hash = HashFunc.hashDataBox(columnValue, 1);
            // 取模得到要使用的分区
            int partitionNum = hash % partitions.length;
            if (partitionNum < 0)  // 哈希可能是负数
                partitionNum += partitions.length;
            partitions[partitionNum].add(record);
        }
    }

    /**
     * 使用leftRecords构建哈希表，并用rightRecords中的记录探测它。
     * 连接匹配的记录并将其作为joinedRecords列表返回。
     *
     * @param partition 一个分区
     * @param rightRecords 右关系中的记录可迭代对象
     */
    private void buildAndProbe(Partition partition, Iterable<Record> rightRecords) {
        // 一个页面用于流式加载 右表数据
        // 一个页面用于输出到 RUN
        // B - 2 个页面 用于加载 Partition 到内存的Hash表

        if (partition.getNumPages() > this.numBuffers - 2) {
            throw new IllegalArgumentException(
                    "此分区中的记录无法适应B-2页内存。"
            );
        }

        // 我们要构建的哈希表。列表包含所有哈希到相同键的左记录
        Map<DataBox, List<Record>> hashTable = new HashMap<>();

        // 构建阶段
        for (Record leftRecord: partition) {
            // 获取左记录的连接值
            DataBox leftJoinValue = leftRecord.getValue(this.getLeftColumnIndex());

            // 创建一个 Hash Bin
            if (!hashTable.containsKey(leftJoinValue)) {
                hashTable.put(leftJoinValue, new ArrayList<>());
            }

            // 把记录添加到对应的 Hash Bin
            hashTable.get(leftJoinValue).add(leftRecord);
        }

        // 探测阶段, 流式加载右表数据，将连接的结果写入 Run
        for (Record rightRecord: rightRecords) {

            // 获取右记录的连接值
            DataBox rightJoinValue = rightRecord.getValue(getRightColumnIndex());

            // 如果没有匹配的键，则跳过
            if (!hashTable.containsKey(rightJoinValue)) continue;


            // 我们必须将右记录与每个具有匹配键的左记录连接
            // 遍历对应的 Hash Bin
            for (Record lRecord : hashTable.get(rightJoinValue)) {
                Record joinedRecord = lRecord.concat(rightRecord);
                // 在this.joinedRecords中累积连接的记录
                this.joinedRecords.add(joinedRecord);
            }
        }
    }

    /**
     * 运行简单哈希连接算法。首先，运行分区阶段创建分区数组。
     * 然后，使用每个哈希分区记录进行构建和探测。
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("已达到最大传递次数");

        // 创建空分区
        Partition[] partitions = createPartitions();

        // Pass1 将左边的记录进行分区
        this.partition(partitions, leftRecords);

        // Pass2 分别加载每个分区，和右表所有记录进行连接
        for (int i = 0; i < partitions.length; i++) {
            buildAndProbe(partitions[i], rightRecords);
        }
    }

    /**
     * 根据我们拥有的可用缓冲区数量创建适当数量的分区并返回数组
     * N = B - 1
     * @return 分区数组
     */
    private Partition[] createPartitions() {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            Schema schema = getLeftSource().getSchema();
            partitions[i] = new Partition(getTransaction(), schema);
        }
        return partitions;
    }
}

