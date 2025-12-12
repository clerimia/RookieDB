package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.disk.Partition;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class GHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    public GHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.GHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // 由于此算法在某些输入上可能会失败，我们给予它最大可能的成本以鼓励优化器避免使用它
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // 在没有协程的情况下实时执行GHJ非常繁琐，因此我们将把所有连接的记录累积到这个运行中
            // 并在算法完成后返回其上的迭代器
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
     * 对于给定迭代器中的每条记录，对其连接列上的值进行哈希处理，
     * 并将其添加到分区中的正确分区。
     *
     * @param partitions 要将记录分割到的分区数组
     * @param records 我们想要分区的记录的可迭代对象
     * @param left 如果记录来自左关系则为true，否则为false
     * @param pass 当前传递次数（用于选择哈希函数）
     */
    private void partition(Partition[] partitions, Iterable<Record> records, boolean left, int pass) {
        // TODO(proj3_part1): 实现分区逻辑
        // 您可能会发现SHJOperator.java中的实现是一个很好的起点。
        // 您可以使用静态方法HashFunc.hashDataBox来获取哈希值。
        // pass 代表进行多次分区
        // 进行分区
        for (Record record : records) {
            // 获取连接值
            DataBox columnValue = record.getValue(left ? getLeftColumnIndex() : getRightColumnIndex());
            // 进行哈希
            int hash = HashFunc.hashDataBox(columnValue, pass);
            // 取模得到要使用的分区
            int partitionNum = hash % partitions.length;
            if (partitionNum < 0)  // 哈希可能是负数
                partitionNum += partitions.length;
            // 插入到对应的分区
            partitions[partitionNum].add(record);
        }
    }

    /**
     * 在给定分区上运行构建和探测阶段。应该将探测阶段找到的任何匹配记录添加到this.joinedRecords。
     */
    private void buildAndProbe(Partition leftPartition, Partition rightPartition) {
        // 如果探测记录来自左分区则为true，否则为false
        boolean probeFirst;
        // 我们将用这些记录在内存中构建哈希表
        Iterable<Record> buildRecords;
        // 我们将用这些记录探测表
        Iterable<Record> probeRecords;
        // 构建记录的连接列索引
        int buildColumnIndex;
        // 探测记录的连接列索引
        int probeColumnIndex;

        if (leftPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = leftPartition;
            buildColumnIndex = getLeftColumnIndex();
            probeRecords = rightPartition;
            probeColumnIndex = getRightColumnIndex();
            probeFirst = false;
        } else if (rightPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = rightPartition;
            buildColumnIndex = getRightColumnIndex();
            probeRecords = leftPartition;
            probeColumnIndex = getLeftColumnIndex();
            probeFirst = true;
        } else {
            throw new IllegalArgumentException(
                "此分区中的左记录和右记录都不适合B-2页内存。"
            );
        }
        // 您不应该在这里引用任何以"left"或"right"开头的变量，
        // 使用我们为您设置的"build"和"probe"变量。
        // 如果您感到困惑，请查看SHJOperator如何实现此功能。
        // 要看哪个表的分区能够直接加载到内存

        Map<DataBox, List<Record>> hashTable = new HashMap<>();
        // 构建阶段
        for (Record buildRecord : buildRecords) {
            // 对于内存中的表，加载到内存中构建哈希表

            // 获取连接值
            DataBox columnValue = buildRecord.getValue(buildColumnIndex);

            // 创建一个 Hash Bin
            if (!hashTable.containsKey(columnValue)) {
                hashTable.put(columnValue, new ArrayList<>());
            }

            // 加入到对应的 Hash Bin
            hashTable.get(columnValue).add(buildRecord);
        }

        // 探测阶段
        for (Record probeRecord : probeRecords) {

            // 获取探测表的连接值
            DataBox columnValue = probeRecord.getValue(probeColumnIndex);

            // 到hash表中进行匹配
            if (!hashTable.containsKey(columnValue)) continue;

            // 匹配到了，进行连接
            for (Record buildRecord : hashTable.get(columnValue)) {
                Record joinRecord = probeFirst ? probeRecord.concat(buildRecord) : buildRecord.concat(probeRecord);
                this.joinedRecords.add(joinRecord);
            }

        }
    }

    /**
     * 运行优雅哈希连接算法。每次传递都从分区开始
     * leftRecords和rightRecords。
     * 如果我们可以在分区上运行构建和探测，
     * 我们应该立即执行，否则我们应该递归地应用优雅哈希连接算法进一步分解分区。
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("已达到最大传递次数");

        // 创建空分区
        Partition[] leftPartitions = createPartitions(true);
        Partition[] rightPartitions = createPartitions(false);

        // 将记录分区到左和右
        this.partition(leftPartitions, leftRecords, true, pass);
        this.partition(rightPartitions, rightRecords, false, pass);

        for (int i = 0; i < leftPartitions.length; i++) {
            // 如果您满足运行构建和探测的条件，您应该立即执行。
            // 否则您应该进行递归调用。
            if (leftPartitions[i].getNumPages() <= this.numBuffers - 2 || rightPartitions[i].getNumPages() <= this.numBuffers - 2) {
                buildAndProbe(leftPartitions[i], rightPartitions[i]);
            } else {
                // 递归调用
                run(leftPartitions[i], rightPartitions[i], pass + 1);
            }
        }
    }

    // 提供的帮助方法 ////////////////////////////////////////////////////////

    /**
     * 根据我们拥有的可用缓冲区数量创建适当数量的分区。
     *
     * @return 分区数组
     */
    private Partition[] createPartitions(boolean left) {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition(left);
        }
        return partitions;
    }

    /**
     * 根据this.useSmartPartition的值创建常规分区或智能分区。
     * @param left 如果此分区将存储来自左关系的记录则为true，否则为false
     * @return 用于存储指定分区记录的分区
     */
    private Partition createPartition(boolean left) {
        Schema schema = getRightSource().getSchema();
        if (left) schema = getLeftSource().getSchema();
        return new Partition(getTransaction(), schema);
    }

    // 学生输入方法 ///////////////////////////////////////////////////

    /**
     * 使用val作为单个int类型列的值创建记录。
     * 添加一个包含500字节字符串的额外列，以便每个页面正好容纳8条记录。
     *
     * @param val 字段将采用的值
     * @return 记录
     */
    private static Record createRecord(int val) {
        String s = new String(new char[500]);
        return new Record(val, s);
    }

    /**
     * 此方法在testBreakSHJButPassGHJ中调用。
     *
     * 提出两个记录列表leftRecords和rightRecords，
     * 使得SHJ在处理这些关系时会出错，但GHJ会成功运行。
     * createRecord(int val)接受一个整数值并返回一条记录，
     * 该记录在连接列上具有该值。
     *
     * 提示：两个连接都将访问B=6个缓冲区，每个页面可以恰好容纳8条记录。
     * 也就是哈希表能够容纳32条记录
     *
     * @return leftRecords和rightRecords的配对
     */
    public static Pair<List<Record>, List<Record>> getBreakSHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            leftRecords.add(createRecord(1));
            rightRecords.add(createRecord(2));
        }
        // 使得SHJ在尝试连接它们时会中断，但GHJ不会
        return new Pair<>(leftRecords, rightRecords);
    }

    /**
     * 此方法在testGHJBreak中调用。
     *
     * 提出两个记录列表leftRecords和rightRecords，
     * 使得GHJ会出错（在我们的情况下是达到最大传递次数）。
     * createRecord(int val)接受一个整数值并返回一条记录，
     * 该记录在连接列上具有该值。
     *
     * 提示：两个连接都将访问B=6个缓冲区，每个页面可以恰好容纳8条记录。
     *
     * @return leftRecords和rightRecords的配对
     */
    public static Pair<List<Record>, List<Record>> getBreakGHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            leftRecords.add(createRecord(1));
            rightRecords.add(createRecord(1));
        }
        return new Pair<>(leftRecords, rightRecords);
    }
}

