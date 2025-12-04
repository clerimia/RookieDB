package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.PageDirectory;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据库中的每个表都维护一组表统计信息，每当向表中添加或删除元组时都会更新这些统计信息。
 * 这些表统计信息包括表中记录的估计数量、表使用的估计页数以及表每列的直方图。
 * 例如，我们可以构造一个TableStats对象并从中添加/删除记录，如下所示：
 *
 *   // 为具有列(x: int, y: float)的表创建TableStats对象
 *   List<String> fieldNames = Arrays.asList("x", "y");
 *   List<Type> fieldTypes = Arrays.asList(Type.intType(), Type.floatType());
 *   Schema schema = new Schema(fieldNames, fieldTypes);
 *   TableStats stats = new TableStats(schema);
 *
 *   // 从统计信息中添加和删除元组
 *   IntDataBox x1 = new IntDataBox(1);
 *   FloatDataBox y1 = new FloatDataBox(1);
 *   Record r1 = new Record(schema, Arrays.asList(x1, y1));
 *
 *   IntDataBox x2 = new IntDataBox(1);
 *   FloatDataBox y2 = new FloatDataBox(1);
 *   Record r2 = new Record(schema, Arrays.asList(x2, y2));
 *
 *   stats.addRecord(r1);
 *   stats.addRecord(r2);
 *   stats.removeRecord(r1);
 *
 * 后来，我们可以使用TableStats对象维护的统计信息进行查询优化等操作：
 *
 *   stats.getNumRecords(); // 记录的估计数量
 *   stats.getNumPages();   // 页面的估计数量
 *   stats.getHistograms(); // 每列的直方图
 */
public class TableStats {
    private Schema schema;
    private int numRecordsPerPage;
    private int numRecords;
    private List<Histogram> histograms;

    /** 为具有模式'schema'的空表构造TableStats */
    public TableStats(Schema schema, int numRecordsPerPage) {
        this.schema = schema;
        this.numRecordsPerPage = numRecordsPerPage;
        this.numRecords = 0;
        this.histograms = new ArrayList<>();
        for (Type t : schema.getFieldTypes()) {
            Histogram h = new Histogram();
            this.histograms.add(h);
        }
    }

    private TableStats(Schema schema, int numRecordsPerPage, int numRecords,
                       List<Histogram> histograms) {
        this.schema = schema;
        this.numRecordsPerPage = numRecordsPerPage;
        this.numRecords = numRecords;
        this.histograms = histograms;
    }

    // 修改器 /////////////////////////////////////////////////////////////////
    public void addRecord(Record record) {
        numRecords++;
    }

    public void removeRecord(Record record) {
        numRecords = Math.max(numRecords - 1, 0);
    }

    public void refreshHistograms(int buckets, Table table) {
        List<Histogram> newHistograms = new ArrayList<>();
        int totalRecords = 0;
        for (int i = 0; i < schema.size(); i++) {
            Histogram h = new Histogram(buckets);
            h.buildHistogram(table, i);
            newHistograms.add(h);
            totalRecords += h.getCount();
        }
        this.histograms = newHistograms;
        this.numRecords = Math.round(((float) totalRecords) / schema.size());
    }

    // 访问器 /////////////////////////////////////////////////////////////////
    public Schema getSchema() { return schema; }

    public int getNumRecords() {
        return numRecords;
    }

    /**
     * 计算存储'numRecords'条记录所需的數據頁面數量
     * 假设所有记录都尽可能密集地存储在页面中
     */
    public int getNumPages() {
        if (numRecords % numRecordsPerPage == 0) return numRecords / numRecordsPerPage;
        return (numRecords / numRecordsPerPage) + 1;
    }

    public List<Histogram> getHistograms() {
        return histograms;
    }

    // 复制器 ///////////////////////////////////////////////////////////////////
    /**
     * 估计应用过滤条件后产生的表的统计信息
     * 过滤条件为第'i'列使用'predicate'和'value'进行过滤
     * 为了简化，我们假设各列之间完全不相关
     * 例如，想象一个表T(x:int, y:int)具有以下表统计信息：
     *
     *   numRecords = 100
     *   numPages = 2
     *               直方图 x                          直方图 y
     *               ===========                         ===========
     *   60 |                       50       60 |
     *   50 |        40           +----+     50 |
     *   40 |      +----+         |    |     40 |
     *   30 |      |    |         |    |     30 |   20   20   20   20   20
     *   20 |   10 |    |         |    |     20 | +----+----+----+----+----+
     *   10 | +----+    | 00   00 |    |     10 | |    |    |    |    |    |
     *   00 | |    |    +----+----+    |     00 | |    |    |    |    |    |
     *       ----------------------------        ----------------------------
     *        0    1    2    3    4    5          0    1    2    3    4    5
     *              0    0    0    0    0               0    0    0    0    0
     *
     * 如果我们应用过滤条件'x < 20'，我们估计将得到以下表统计信息：
     *
     *   numRecords = 50
     *   numPages = 1
     *               直方图 x                          直方图 y
     *               ===========                         ===========
     *   50 |        40                      50 |
     *   40 |      +----+                    40 |
     *   30 |      |    |                    30 |
     *   20 |   10 |    |                    20 |   10   10   10   10   10
     *   10 | +----+    | 00   00   00       10 | +----+----+----+----+----+
     *   00 | |    |    +----+----+----+     00 | |    |    |    |    |    |
     *       ----------------------------        ----------------------------
     *        0    1    2    3    4    5          0    1    2    3    4    5
     *              0    0    0    0    0               0    0    0    0    0
     */
    public TableStats copyWithPredicate(int column,
                                        PredicateOperator predicate,
                                        DataBox d) {
        float reductionFactor = histograms.get(column).computeReductionFactor(predicate, d);
        List<Histogram> copyHistograms = new ArrayList<>();
        for (int j = 0; j < histograms.size(); j++) {
            Histogram histogram = histograms.get(j);
            if (column == j) {
                // 对于目标列，直接应用谓词
                copyHistograms.add(histogram.copyWithPredicate(predicate, d));
            } else {
                // 对于其他列，按缩减因子减少
                copyHistograms.add(histogram.copyWithReduction(reductionFactor));
            }
        }
        int numRecords = copyHistograms.get(column).getCount();
        return new TableStats(this.schema, this.numRecordsPerPage, numRecords, copyHistograms);
    }

    /**
     * 创建一个新的TableStats，表示此TableStats与给定TableStats连接后产生的表的统计信息
     *
     * @param leftIndex 此表连接列的索引
     * @param rightStats 要连接的右表的TableStats
     * @param rightIndex 右表连接列的索引
     * @return 基于此对象和参数的新TableStats
     */
    public TableStats copyWithJoin(int leftIndex,
                                   TableStats rightStats,
                                   int rightIndex) {
        // 计算新的模式
        Schema joinedSchema = this.schema.concat(rightStats.schema);
        int inputSize = this.numRecords * rightStats.numRecords;
        int leftNumDistinct = 1;
        if (this.histograms.size() > 0) {
            leftNumDistinct = this.histograms.get(leftIndex).getNumDistinct() + 1;
        }

        int rightNumDistinct = 1;
        if (rightStats.histograms.size() > 0) {
            rightNumDistinct = rightStats.histograms.get(rightIndex).getNumDistinct() + 1;
        }

        float reductionFactor = 1.0f / Math.max(leftNumDistinct, rightNumDistinct);
        List<Histogram> copyHistograms = new ArrayList<>();

        float leftReductionFactor = leftNumDistinct * reductionFactor;
        float rightReductionFactor = rightNumDistinct * reductionFactor;
        int outputSize = (int)(reductionFactor * inputSize);

        for (Histogram leftHistogram : this.histograms) {
            copyHistograms.add(leftHistogram.copyWithJoin(outputSize, leftReductionFactor));
        }

        for (Histogram rightHistogram : rightStats.histograms) {
            copyHistograms.add(rightHistogram.copyWithJoin(outputSize, rightReductionFactor));
        }

        int joinedRecordsPerPage = Table.computeNumRecordsPerPage(
                PageDirectory.EFFECTIVE_PAGE_SIZE, joinedSchema);
        return new TableStats(joinedSchema, joinedRecordsPerPage, outputSize, copyHistograms);
    }
}
