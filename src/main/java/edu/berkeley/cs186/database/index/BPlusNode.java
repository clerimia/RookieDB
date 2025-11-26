package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.util.Iterator;
import java.util.Optional;

/**
 * 内部节点或叶节点。有关更多信息，请参见InnerNode和LeafNode。
 * B+树节点的抽象，有两种不同的节点：内部节点 + 叶子节点
 */
abstract class BPlusNode {
    // 核心API ////////////////////////////////////////////////////////////////
    /**
     * n.get(k) 返回从n查询时k可能所在的叶节点。
     * 例如，考虑以下B+树（为了简洁，只显示键；省略了记录ID）。
     *
     *                               内部节点
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * inner.get(x) 应该返回
     *
     *   - 当 x < 10 时，返回 leaf0，
     *   - 当 10 <= x < 20 时，返回 leaf1，
     *   - 当 x >= 20 时，返回 leaf2。
     *
     * 注意，即使leaf0实际上不包含4，inner.get(4)也会返回leaf0。
     */
    public abstract LeafNode get(DataBox key);

    /**
     * n.getLeftmostLeaf() 返回以n为根的子树中最左边的叶节点。
     * 在上面的例子中，inner.getLeftmostLeaf()将返回leaf0，
     * leaf1.getLeftmostLeaf()将返回leaf1。
     */
    public abstract LeafNode getLeftmostLeaf();

    /**
     * n.put(k, r) 将键值对(k, r)插入到以n为根的子树中。需要考虑两种情况：
     *
     *   情况1: 如果插入键值对(k, r)不会导致n溢出，则返回Optional.empty()。
     *   情况2: 如果插入键值对(k, r)导致节点n溢出，则将n拆分为左右两个节点（如下所述），
     *         并返回一个键值对(split_key, right_node_page_num)，其中right_node_page_num
     *         是新创建的右节点的页号，而split_key的值取决于n是内部节点还是叶节点（如下所述）。
     *
     * 现在我们解释如何拆分节点以及返回哪个拆分键。让我们看一个例子。
     * 考虑将键4插入到上面的示例树中。没有节点溢出（即我们总是遇到情况1）。
     * 树看起来像这样：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |  4 |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * 现在让我们将键5插入到树中。现在，leaf0溢出并创建一个新的右侧兄弟节点leaf3。
     * d个条目保留在左节点中；d+1个条目移动到右节点中。请勿以其他任何方式重新分配条目。
     * 在我们的例子中，leaf0和leaf3看起来像这样：
     *
     *   +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |    |    |->|  3 |  4 |  5 |    |
     *   +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf3
     *
     * 当叶节点拆分时，它将右节点中的第一个条目作为拆分键返回。在此示例中，3是拆分键。
     * leaf0拆分后，inner将新键和子指针插入自身并遇到情况1（即它不会溢出）。
     * 树看起来像这样：
     * 拆分要满足节点的entry数 >= d 这里的d是2
     *
     *                          inner
     *                          +--+--+--+--+
     *                          | 3|10|20|  |
     *                          +--+--+--+--+
     *                         /   |  |   \
     *                 _______/    |  |    \_________
     *                /            |   \             \
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   | 1| 2|  |  |->| 3| 4| 5|  |->|11|12|13|  |->|21|22|23|  |
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   leaf0          leaf3          leaf1          leaf2
     *
     * 当内部节点拆分时，前d个条目保留在左节点中，最后d个条目移动到右节点中。 如果节点满了才需要进行拆分
     * 中间的条目作为拆分键向上移动（而不是复制）。例如，我们会将以下顺序为2的内部节点
     *
     *   +---+---+---+---+
     *   | 1 | 2 | 3 | 4 | 5
     *   +---+---+---+---+
     *
     * 拆分为以下两个内部节点
     *
     *   +---+---+---+---+  +---+---+---+---+
     *   | 1 | 2 |   |   |  | 4 | 5 |   |   |
     *   +---+---+---+---+  +---+---+---+---+
     *
     * 拆分键为3。
     * 拆分后，右边的第一个节点将向上"提"变成索引项的一部分
     *
     * 除了我们描述的方式外，请勿以任何其他方式重新分配条目。
     * 例如，不要在节点之间移动条目以避免拆分。
     *
     * 我们的B+树不支持具有相同键的重复条目。
     * 如果将重复键插入叶节点，则树保持不变并抛出BPlusTreeException异常。
     */
    public abstract Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid);

    /**
     * n.bulkLoad(data, fillFactor) 使用给定的填充因子将数据中的(k, r)键值对批量加载到树中。
     *
     * 此方法与n.put非常相似，但有几点不同：
     *
     * 1. 叶节点不会填满到2*d+1然后拆分，而是填充到比fillFactor多一个记录的程度，
     *    然后通过创建一个只包含一个记录的右侧兄弟节点来进行"拆分"
     *    （使原节点达到所需的填充因子）。
     *
     * 2. 内部节点应重复尝试批量加载最右边的子节点，直到内部节点已满（在这种情况下应该拆分）
     *    或者没有更多数据为止。
     *
     * fillFactor应该仅用于确定叶节点的填充程度（不适用于内部节点），
     * 计算应该向上取整，即当d=5且fillFactor=0.75时，叶节点应该是8/10满。
     *
     * 您可以假设测试时0 < fillFactor <= 1，并且超出该范围的填充因子将导致未定义的行为
     * （您可以自由处理这些情况）。
     */
    public abstract Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor);

    /**
     * n.remove(k) 从以n为根的子树中删除键k及其对应的记录ID，
     * 如果键k不在子树中则不执行任何操作。
     * 删除时不应重新平衡树。只需删除键和对应的记录ID。
     * 例如，在上面的示例树上运行inner.remove(2)将产生以下树：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  3 |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * 在此树上运行inner.remove(1)将产生以下树：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  3 |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * 然后运行inner.remove(3)将产生以下树：
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |    |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * 再次强调，不要重新平衡树。
     */
    public abstract void remove(DataBox key);

    // Helpers /////////////////////////////////////////////////////////////////
    /** 获取此节点持久化的页面。 */
    abstract Page getPage();

    // Pretty Printing /////////////////////////////////////////////////////////
    /**
     * S表达式（或sexp）是一种紧凑的编码嵌套树状结构的方式
     * （有点像JSON是编码嵌套字典和列表的方式）。
     * n.toSexp()返回以n为根的子树的sexp编码。
     * 例如，以下树：
     *
     *                      +---+
     *                      | 3 |
     *                      +---+
     *                     /     \
     *   +---------+---------+  +---------+---------+
     *   | 1:(1 1) | 2:(2 2) |  | 3:(3 3) | 4:(4 4) |
     *   +---------+---------+  +---------+---------+
     *
     * 有以下sexp
     *
     *   (((1 (1 1)) (2 (2 2))) 3 ((3 (3 3)) (4 (4 4))))
     *
     * 这里，(1 (1 1))表示从键1到记录ID(1, 1)的映射。
     */
    public abstract String toSexp();

    /**
     * n.toDot() 返回绘制以n为根的子树的DOT文件片段。
     */
    public abstract String toDot();

    // Serialization ///////////////////////////////////////////////////////////
    /** n.toBytes() 序列化n。 */
    public abstract byte[] toBytes();

    /**
     * BPlusNode.fromBytes(m, p) 从页面`pageNum`加载BPlusNode。
     */
    public static BPlusNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                      LockContext treeContext, long pageNum) {
        Page p = bufferManager.fetchPage(treeContext, pageNum);
        try {
            Buffer buf = p.getBuffer();
            byte b = buf.get();
            if (b == 1) {
                return LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else if (b == 0) {
                return InnerNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else {
                String msg = String.format("Unexpected byte %b.", b);
                throw new IllegalArgumentException(msg);
            }
        } finally {
            p.unpin();
        }
    }
}