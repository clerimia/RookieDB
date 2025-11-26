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
 * B+树的叶节点。每个阶数为d的B+树叶节点存储d到2d个（键，记录ID）对和指向其右兄弟节点的指针
 *（即其右兄弟节点的页号）。此外，每个叶节点都在单个页面上序列化和持久化；
 * 有关叶节点如何序列化的详细信息，请参见toBytes和fromBytes。
 * 例如，这里是两个连接在一起的阶数为2的叶节点的示例：
 *
 *   leaf 1 (stored on some page)          leaf 2 (stored on some other page)
 *   +-------+-------+-------+-------+     +-------+-------+-------+-------+
 *   | k0:r0 | k1:r1 | k2:r2 |       | --> | k3:r3 | k4:r4 |       |       |
 *   +-------+-------+-------+-------+     +-------+-------+-------+-------+
 */
class LeafNode extends BPlusNode {
    // 此节点所属的B+树的元数据。
    private BPlusTreeMetadata metadata;

    // 缓冲区管理器
    private BufferManager bufferManager;

    // B+树的锁上下文
    private LockContext treeContext;

    // 此叶节点序列化的页面。
    private Page page;

    // 此叶节点的键和记录ID。`keys`始终按升序排序。
    // 索引i处的记录ID对应于索引i处的键。
    // 例如，键[a, b, c]和记录ID[1, 2, 3]表示配对[a:1, b:2, c:3]。
    //
    // 注意以下细微差别。keys和rids是存储在磁盘上的键和记录ID的内存缓存。
    // 因此，考虑当您创建指向同一页的两个LeafNode对象时会发生什么：
    //
    //   BPlusTreeMetadata meta = ...;
    //   int pageNum = ...;
    //   LockContext treeContext = new DummyLockContext();
    //
    //   LeafNode leaf0 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
    //   LeafNode leaf1 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
    //
    // 这种情况看起来像这样：
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2]     | | | k0:r0 | k1:r1 | k2:r2 |       |
    //   | rids = [r0, r1, r2]     | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // 现在想象我们对leaf0执行操作，如leaf0.put(k3, r3)。
    // leaf0的内存值将被更新并同步到磁盘。
    // 但是，leaf1的内存值不会被更新。那将看起来像这样：
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2, k3] | | | k0:r0 | k1:r1 | k2:r2 | k3:r3 |
    //   | rids = [r0, r1, r2, r3] | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // 确保您的代码（或测试）不使用过时的内存缓存键和记录ID值。
    private List<DataBox> keys;
    private List<RecordId> rids;

    // 如果此叶节点是最右边的叶节点，则rightSibling为Optional.empty()。
    // 否则，rightSibling为Optional.of(n)，其中n是此叶节点右兄弟节点的页号。
    private Optional<Long> rightSibling;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * 构造一个全新的叶节点。此构造函数将从提供的BufferManager `bufferManager`中获取一个新的已固定页面，
     * 并将节点持久化到该页面。
     */
    LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
             List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
             keys, rids,
             rightSibling, treeContext);
    }

    /**
     * 构造一个持久化到页面`page`的叶节点。
     */
    private LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                     List<DataBox> keys,
                     List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        try {
            assert (keys.size() == rids.size());
            assert (keys.size() <= 2 * metadata.getOrder());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.rids = new ArrayList<>(rids);
            this.rightSibling = rightSibling;

            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    // 参见 BPlusNode.get。
    @Override
    public LeafNode get(DataBox key) {
        return this;
    }

    // 参见 BPlusNode.getLeftmostLeaf。
    @Override
    public LeafNode getLeftmostLeaf() {
        return this;
    }

    // 参见 BPlusNode.put。
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // 1.查找插入位置并检查重复键
        int index = 0;
        for (index = 0; index < keys.size(); index++) {
            if (key.compareTo(keys.get(index)) == 0) {
                throw new BPlusTreeException("Duplication!!!!");
            }
            if (key.compareTo(keys.get(index)) < 0) {
                break;
            }
        }

        // 2.插入键值对
        keys.add(index, key);
        rids.add(index, rid);

        // 3.未溢出 keys.size() <= 2d
        if (keys.size() <= 2 * metadata.getOrder()) {
            sync();
            return Optional.empty();
        } else {
            // 4.溢出 需要分裂
            // 分裂点应该是中间位置
            int splitIndex = metadata.getOrder();

            // 5.创建右节点包含的键和记录
            List<DataBox> newKeys = new ArrayList<>(keys.subList(splitIndex, keys.size()));
            List<RecordId> newRids = new ArrayList<>(rids.subList(splitIndex, rids.size()));

            // 6.更新当前节点只保留前splitIndex个元素
            keys = new ArrayList<>(keys.subList(0, splitIndex));
            rids = new ArrayList<>(rids.subList(0, splitIndex));

            // 7.创建新的leafNode
            LeafNode newLeafNode = new LeafNode(metadata, bufferManager, newKeys, newRids, rightSibling, treeContext);

            // 8.更新叶子节点的兄弟指针
            long newLeafNodePageNum = newLeafNode.page.getPageNum();
            rightSibling = Optional.of(newLeafNodePageNum);

            // 9.刷盘
            sync();

            // 10.返回提升的键和新节点页号 (应该是新节点的第一个键)
            return Optional.of(new Pair<>(newKeys.get(0), newLeafNodePageNum));
        }


    }

    // 参见 BPlusNode.bulkLoad。
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor) {
        // TODO(proj2): implement
        // 1. 插入，直到 = 2 * d + 1
        while (data.hasNext() && keys.size() <= metadata.getOrder() * 2) {
            Pair<DataBox, RecordId> pair = data.next(); // 注意next只能调用一次
            keys.add(pair.getFirst());
            rids.add(pair.getSecond());
        }
        // 2. 结束循环判断
        int size = keys.size();
        if (size > metadata.getOrder() * 2) {
            // 3. 开始分裂
            int splitIndex = (int) (metadata.getOrder() * 2 * fillFactor);
            // 4. 进行copy up
            // 4.1 创建新叶子节点
            ArrayList<DataBox> newKeys = new ArrayList<>(keys.subList(splitIndex, size));
            ArrayList<RecordId> newRids = new ArrayList<>(rids.subList(splitIndex, size));
            LeafNode leafNode = new LeafNode(metadata, bufferManager, newKeys, newRids, rightSibling, treeContext);

            // 4.2 更新当前节点
            keys = new ArrayList<>(keys.subList(0, splitIndex));
            rids = new ArrayList<>(rids.subList(0, splitIndex));
            long pageNum = leafNode.page.getPageNum();
            rightSibling = Optional.of(pageNum);

            // 4.4 刷盘
            sync();
            // 4.4 copy up
            return Optional.of(new Pair<>(newKeys.get(0), pageNum));
        }

        // 刷盘
        sync();
        return Optional.empty();
    }

    // 参见 BPlusNode.remove。
    @Override
    public void remove(DataBox key) {
        // 1.find index
        int index = keys.indexOf(key);

        // 2.remove
        if (index != -1) {
            keys.remove(index);
            rids.remove(index);
        }

        // 3.记住刷盘
        sync();
    }

    // Iterators ///////////////////////////////////////////////////////////////
    /** 返回与`key`关联的记录ID。 */
    Optional<RecordId> getKey(DataBox key) {
        int index = keys.indexOf(key);
        return index == -1 ? Optional.empty() : Optional.of(rids.get(index));
    }

    /**
     * 返回此叶节点记录ID的迭代器，按其对应键的升序排列。
     */
    Iterator<RecordId> scanAll() {
        return rids.iterator();
    }

    /**
     * 返回此叶节点记录ID的迭代器，这些记录ID具有大于或等于`key`的对应键。
     * 记录ID按其对应键的升序返回。
     */
    Iterator<RecordId> scanGreaterEqual(DataBox key) {
        int index = InnerNode.numLessThan(key, keys);
        return rids.subList(index, rids.size()).iterator();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    /** 返回此叶节点的右兄弟节点（如果有的话）。 */
    Optional<LeafNode> getRightSibling() {
        if (!rightSibling.isPresent()) {
            return Optional.empty();
        }

        long pageNum = rightSibling.get();
        return Optional.of(LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum));
    }

    /** 将此叶节点序列化到其页面。 */
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
    List<RecordId> getRids() {
        return rids;
    }

    /**
     * 返回最大数字d，使得具有2d个条目的LeafNode的序列化将适合单个页面。
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // 具有n个条目的叶节点占用以下字节数：
        //
        //   1 + 8 + 4 + n * (keySize + ridSize)
        //
        // 其中
        //
        //   - 1是用于存储isLeaf的字节数，
        //   - 8是用于存储兄弟指针的字节数，
        //   - 4是用于存储n的字节数，
        //   - keySize是用于存储类型为keySchema的DataBox的字节数，以及
        //   - ridSize是RecordId的字节数。
        //
        // 解以下方程
        //
        //   n * (keySize + ridSize) + 13 <= pageSizeInBytes
        //
        // 我们得到
        //
        //   n = (pageSizeInBytes - 13) / (keySize + ridSize)
        //
        // 阶数d是n的一半。
        int keySize = keySchema.getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + ridSize);
        return n / 2;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        String rightSibString = rightSibling.map(Object::toString).orElse("None");
        return String.format("LeafNode(pageNum=%s, keys=%s, rids=%s, rightSibling=%s)",
                page.getPageNum(), keys, rids, rightSibString);
    }

    @Override
    public String toSexp() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i).toString();
            String rid = rids.get(i).toSexp();
            ss.add(String.format("(%s %s)", key, rid));
        }
        return String.format("(%s)", String.join(" ", ss));
    }

    /**
     * 给定一个具有页号1和三个（键，记录ID）对(0, (0, 0))、(1, (1, 1))和(2, (2, 2))的叶节点，
     * 相应的dot片段是：
     *
     *   node1[label = "{0: (0 0)|1: (1 1)|2: (2 2)}"];
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("%s: %s", keys.get(i), rids.get(i).toSexp()));
        }
        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        return String.format("  node%d[label = \"{%s}\"];", pageNum, s);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // 当我们序列化叶节点时，我们写入：
        //
        //   a. 字面值1（1字节），表示此节点是叶节点，
        //   b. 我们右兄弟节点的页ID（8字节）（如果没有右兄弟节点，则为-1），
        //   c. 此叶节点包含的（键，记录ID）对的数量（4字节），以及
        //   d. （键，记录ID）对本身。
        //
        // 例如，以下字节：
        //
        //   +----+-------------------------+-------------+----+-------------------------------+
        //   | 01 | 00 00 00 00 00 00 00 04 | 00 00 00 01 | 03 | 00 00 00 00 00 00 00 03 00 01 |
        //   +----+-------------------------+-------------+----+-------------------------------+
        //    \__/ \_______________________/ \___________/ \__________________________________/
        //     a               b                   c                         d
        //
        // 表示一个具有页4上的兄弟节点和单个（键，记录ID）对的叶节点，该对具有键3和页ID（3，1）。

        assert (keys.size() == rids.size());
        assert (keys.size() <= 2 * metadata.getOrder());

        // 所有大小均以字节为单位。
        int isLeafSize = 1;
        int siblingSize = Long.BYTES;
        int lenSize = Integer.BYTES;
        int keySize = metadata.getKeySchema().getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int entriesSize = (keySize + ridSize) * keys.size();
        int size = isLeafSize + siblingSize + lenSize + entriesSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 1);
        buf.putLong(rightSibling.orElse(-1L)); // 如果没有右兄弟节点，就序列化为-1
        buf.putInt(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            buf.put(keys.get(i).toBytes());
            buf.put(rids.get(i).toBytes());
        }
        return buf.array();
    }

    /**
     * 从页面`pageNum`加载叶节点。
     */
    public static LeafNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                     LockContext treeContext, long pageNum) {
        // TODO(proj2): implement 注意没有右节点的情况
        // 1.从页面`pageNum`加载内部节点。
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        // 2.获取指向页面的JVM缓冲区
        Buffer buffer = page.getBuffer();

        // 3.获取节点类型, 并校验
        byte nodeType = buffer.get();
        assert(nodeType == 0x01); // 是叶子节点

        // 4.右兄弟节点的页ID
        long rightSibling = buffer.getLong();

        // 5.读取包含的(ket, recordId)的数量
        int entryNum = buffer.getInt();
        List<DataBox> keys = new ArrayList<>();
        List<RecordId> recordIds = new ArrayList<>();

        // 6.读取entrys
        for (int i = 0; i < entryNum; i++) {
            keys.add(DataBox.fromBytes(buffer, metadata.getKeySchema()));
            recordIds.add(RecordId.fromBytes(buffer));
        }

        // 注意：LeafNode有两个构造函数。要实现fromBytes，请确保使用
        // 重用现有页面而不是获取全新页面的构造函数。

        // 创建内部节点并返回 没有右边节点!!!!
        return new LeafNode(metadata, bufferManager, page, keys, recordIds, rightSibling == -1 ? Optional.empty() : Optional.of(rightSibling), treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LeafNode)) {
            return false;
        }
        LeafNode n = (LeafNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
               keys.equals(n.keys) &&
               rids.equals(n.rids) &&
               rightSibling.equals(n.rightSibling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, rids, rightSibling);
    }
}