package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * B+树的内部节点。每个阶数为d的B+树内部节点存储d到2d个键。
 * 具有n个键的内部节点存储n + 1个指向子节点的"指针"（指针只是页号）。
 * 此外，每个内部节点都在单个页面上序列化和持久化；
 * 有关内部节点如何序列化的详细信息，请参见toBytes和fromBytes。
 * 例如，这里是一个阶数为2的内部节点的示例：
 *
 *     +----+----+----+----+
 *     | 10 | 20 | 30 |    |
 *     +----+----+----+----+
 *    /     |    |     \
 */
class InnerNode extends BPlusNode {
    // 此节点所属的B+树的元数据。
    private BPlusTreeMetadata metadata;

    // 缓冲区管理器
    private BufferManager bufferManager;

    // B+树的锁上下文
    private LockContext treeContext;

    // 此叶节点序列化的页面。
    private Page page;

    // 此内部节点的键和子节点指针。请参阅LeafNode.java中LeafNode.keys和LeafNode.rids上方的注释，
    // 了解此处的键和子节点与存储在磁盘上的键和子节点之间的差异警告。
    // `keys`始终按升序存储。
    private List<DataBox> keys;
    private List<Long> children; // 存储子节点的页号

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * 构造一个全新的内部节点。
     */
    InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
              List<Long> children, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
             keys, children, treeContext);
    }

    /**
     * 构造一个持久化到页面`page`的内部节点。
     */
    private InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                      List<DataBox> keys, List<Long> children, LockContext treeContext) {
        try {
            assert (keys.size() <= 2 * metadata.getOrder());
            assert (keys.size() + 1 == children.size());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.children = new ArrayList<>(children);
            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    // 参见 BPlusNode.get。
    @Override
    public LeafNode get(DataBox key) {
        // 1. 遍历自己的 keys 找到对应key所在的区间
        int index;
        for (index = 0; index < keys.size(); index++) {
            if (key.compareTo(keys.get(index)) < 0) {
                break;
            }
        }

        // 2. 获取对应节点
        BPlusNode childNode = getChild(index);

        // 3. 递归调用
        return childNode.get(key);
    }

    // 参见 BPlusNode.getLeftmostLeaf。
    @Override
    public LeafNode getLeftmostLeaf() {
        assert(children.size() > 0);
        // 1. 获取自己最小节点的页号
        BPlusNode childNode = getChild(0);

        // 2. 递归调用
        return childNode.getLeftmostLeaf();
    }

    // 参见 BPlusNode.put。
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // 内存中操作！！！！！
        // 1.找到自己的子节点
        int index;
        for (index = 0; index < keys.size(); index++) {
            if (key.compareTo(keys.get(index)) < 0) {
                break;
            }
        }
        // 2. 获取对应节点
        BPlusNode childNode = getChild(index);

        // 3. 递归调用
        Optional<Pair<DataBox, Long>> copyUp = childNode.put(key, rid);


        // 3. 处理可能的COPY UP操作
        if (copyUp.isPresent()) {
            DataBox newKey = copyUp.get().getFirst();
            long newPageNum = copyUp.get().getSecond();

            // 3.1 找到新键的插入位置
            int insertIndex = 0;
            for (insertIndex = 0; insertIndex < keys.size(); insertIndex++) {
                if (newKey.compareTo(keys.get(insertIndex)) < 0) {
                    break;
                }
            }

            // 3.2 插入新键和子节点指针
            keys.add(insertIndex, newKey);
            children.add(insertIndex + 1, newPageNum);

            // 3.3 检查是否溢出
            if (keys.size() <= metadata.getOrder() * 2) {
                // 没有溢出，直接刷新返回
                sync();
                return Optional.empty();
            } else {
                // 溢出处理
                // 3.4 找到分割点
                int splitIndex = metadata.getOrder();

                // 3.5 准备新节点的数据
                ArrayList<DataBox> newKeys = new ArrayList<>(keys.subList(splitIndex + 1, keys.size()));
                ArrayList<Long> newChildren = new ArrayList<>(children.subList(splitIndex + 1, children.size()));

                // 3.6 创建新内部节点
                InnerNode newInnerNode = new InnerNode(metadata, bufferManager, newKeys, newChildren, treeContext);

                // 3.7 获取提升的键和新节点页号
                Pair<DataBox, Long> entry = new Pair<>(keys.get(splitIndex), newInnerNode.getPage().getPageNum());

                // 3.8 更新当前节点
                keys = new ArrayList<>(keys.subList(0, splitIndex));
                children = new ArrayList<>(children.subList(0, splitIndex + 1));

                // 3.9 刷盘
                sync();

                // 3.10 返回提升的键值对
                return Optional.of(entry);
            }
        }

        return Optional.empty();
    }

    // 参见 BPlusNode.bulkLoad。
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor) {
        // TODO(proj2): implement
        // 1.判断迭代器中是否还有记录, 以及是否应该分裂了
        while (data.hasNext() && keys.size() <= metadata.getOrder() * 2) {
            // 2.直接往下层最后一个节点插
            BPlusNode lastChild = getChild(keys.size()); // 注意，这个函数是获取index = arg的指针，所以直接size就行，就是最后一个
            Optional<Pair<DataBox, Long>> up = lastChild.bulkLoad(data, fillFactor);

            // 3.判断下层是否有分裂
            if (up.isPresent()) {
                // 有分裂
                // 3.1 获取key 和 指针
                DataBox key = up.get().getFirst();
                long pageNum = up.get().getSecond();

                // 3.2 插入到自己的节点中， 注意，一定是递增的顺序
                keys.add(key);
                children.add(pageNum);
            }
        }

        // 4. 如果应该分裂了
        int size = keys.size();
        if (size > metadata.getOrder() * 2) {
            // 5. 开始分裂
            int splitIndex = metadata.getOrder();
            ArrayList<DataBox> newKeys = new ArrayList<>(keys.subList(splitIndex + 1, keys.size()));
            ArrayList<Long> newChildren = new ArrayList<>(children.subList(splitIndex + 1, children.size()));
            InnerNode newInnerNode = new InnerNode(metadata, bufferManager, newKeys, newChildren, treeContext);

            // 6. move up
            Pair<DataBox, Long> entry = new Pair<>(keys.get(splitIndex), newInnerNode.getPage().getPageNum());

            // 7. 更新当前节点并刷盘
            keys = new ArrayList<>(keys.subList(0, splitIndex));
            children = new ArrayList<>(children.subList(0, splitIndex));
            sync();

            return Optional.of(entry);
        }

        // 8. 不用分裂
        sync();
        return Optional.empty();
    }

    // 参见 BPlusNode.remove。
    @Override
    public void remove(DataBox key) {
        // 1. 找下一层
        int index;
        for (index = 0; index < keys.size(); index++) {
            if (key.compareTo(keys.get(index)) < 0) {
                break;
            }
        }
        BPlusNode childNode = getChild(index);

        // 2. 递归找
        childNode.remove(key);

        /*// 3. 刷盘
        sync();*/
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    private BPlusNode getChild(int i) {
        long pageNum = children.get(i);
        return BPlusNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<Long> getChildren() {
        return children;
    }
    /**
     * 返回最大数字d，使得具有2d个键的InnerNode的序列化将适合单个页面。
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // 具有n个条目的叶节点占用以下字节数：
        //
        //   1 + 4 + (n * keySize) + ((n + 1) * 8)
        //
        // 其中
        //
        //   - 1是用于存储isLeaf的字节数，
        //   - 4是用于存储n的字节数，
        //   - keySize是用于存储类型为keySchema的DataBox的字节数，以及
        //   - 8是用于存储子节点指针的字节数。
        //
        // 解以下方程
        //
        //   5 + (n * keySize) + ((n + 1) * 8) <= pageSizeInBytes
        //
        // 我们得到
        //
        //   n = (pageSizeInBytes - 13) / (keySize + 8)
        //
        // 阶数d是n的一半。
        int keySize = keySchema.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + 8);
        return n / 2;
    }

    /**
     * 给定按升序排序的列表ys，numLessThanEqual(x, ys)返回ys中小于或等于x的元素数量。例如，
     *
     *   numLessThanEqual(0, Arrays.asList(1, 2, 3, 4, 5)) == 0
     *   numLessThanEqual(1, Arrays.asList(1, 2, 3, 4, 5)) == 1
     *   numLessThanEqual(2, Arrays.asList(1, 2, 3, 4, 5)) == 2
     *   numLessThanEqual(3, Arrays.asList(1, 2, 3, 4, 5)) == 3
     *   numLessThanEqual(4, Arrays.asList(1, 2, 3, 4, 5)) == 4
     *   numLessThanEqual(5, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *   numLessThanEqual(6, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *
     * 当我们在B+树中向下导航并需要决定访问哪个子节点时，这个辅助函数很有用。
     * 例如，想象一个具有以下4个键和5个子节点指针的索引节点：
     *
     *     +---+---+---+---+
     *     | a | b | c | d |
     *     +---+---+---+---+
     *    /    |   |   |    \
     *   0     1   2   3     4
     *
     * 如果我们在树中搜索值c，那么我们需要访问子节点3。
     * 不是巧合的是，也有3个值小于或等于c（即a, b, c）。
     */
    static <T extends Comparable<T>> int numLessThanEqual(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) <= 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    static <T extends Comparable<T>> int numLessThan(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) < 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(children.get(i)).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(children.get(children.size() - 1)).append(")");
        return sb.toString();
    }

    @Override
    public String toSexp() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(getChild(i).toSexp()).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(getChild(children.size() - 1).toSexp()).append(")");
        return sb.toString();
    }

    /**
     * 页面0上的内部节点具有单个键k和页面1和2上的两个子节点，转换为以下DOT片段：
     *
     *   node0[label = "<f0>|k|<f1>"];
     *   ... // 子节点
     *   "node0":f0 -> "node1";
     *   "node0":f1 -> "node2";
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("<f%d>", i));
            ss.add(keys.get(i).toString());
        }
        ss.add(String.format("<f%d>", keys.size()));

        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        String node = String.format("  node%d[label = \"%s\"];", pageNum, s);

        List<String> lines = new ArrayList<>();
        lines.add(node);
        for (int i = 0; i < children.size(); ++i) {
            BPlusNode child = getChild(i);
            long childPageNum = child.getPage().getPageNum();
            lines.add(child.toDot());
            lines.add(String.format("  \"node%d\":f%d -> \"node%d\";",
                                    pageNum, i, childPageNum));
        }

        return String.join("\n", lines);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // 当我们序列化内部节点时，我们写入：
        //
        //   a. 字面值0（1字节），表示此节点不是叶节点，
        //   b. 此内部节点包含的键数量n（4字节）（比子节点指针数量少一个），
        //   c. n个键，以及键的序列化形式
        //   d. n+1个子节点指针。
        //
        // 例如，以下字节：
        //
        //   +----+-------------+----+-------------------------+-------------------------+
        //   | 00 | 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03 | 00 00 00 00 00 00 00 07 |
        //   +----+-------------+----+-------------------------+-------------------------+
        //    \__/ \___________/ \__/ \_________________________________________________/
        //     a         b        c                           d
        //
        // 表示一个具有一个键（即1）和两个子节点指针（即页面3和页面7）的内部节点。

        // 所有大小均以字节为单位。
        // 校验节点的阶数
        assert (keys.size() <= 2 * metadata.getOrder());
        assert (keys.size() + 1 == children.size());

        // 计算存储这个节点需要的字节数
        int isLeafSize = 1;  // 内部节点还是叶子节点的标识
        int numKeysSize = Integer.BYTES; // key数量
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size(); // key的类型和所占的字节数
        int childrenSize = Long.BYTES * children.size(); // 指针数组说占的字节数
        int size = isLeafSize + numKeysSize + keysSize + childrenSize; // 这个节点总共的大小

        // 申请内存缓冲区
        ByteBuffer buf = ByteBuffer.allocate(size);
        // 进行序列化
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (Long child : children) {
            buf.putLong(child);
        }
        return buf.array();
    }

    /**
     * 从页面`pageNum`加载内部节点。
     */
    public static InnerNode fromBytes(BPlusTreeMetadata metadata,
                                      BufferManager bufferManager, LockContext treeContext, long pageNum) {
        // 从页面`pageNum`加载内部节点。
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        // 获取指向页面的JVM缓冲区
        Buffer buf = page.getBuffer();

        // 获取节点类型, 并校验
        byte nodeType = buf.get();
        assert(nodeType == (byte) 0);

        // 读取keys 和 指针
        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        // 读取Key的数量
        int n = buf.getInt();
        // 读取Key数组
        for (int i = 0; i < n; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        // 读取指针数组
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getLong());
        }
        // 创建内部节点并返回
        return new InnerNode(metadata, bufferManager, page, keys, children, treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InnerNode)) {
            return false;
        }
        InnerNode n = (InnerNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
               keys.equals(n.keys) &&
               children.equals(n.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, children);
    }
}