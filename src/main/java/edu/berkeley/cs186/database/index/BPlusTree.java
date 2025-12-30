package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.table.RecordId;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * 持久化b+数
 * A persistent B+ tree.
 *
 *   BPlusTree tree = new BPlusTree(bufferManager, metadata, lockContext);
 *
 *   // Insert some values into the tree.
 *   tree.put(new IntDataBox(0), new RecordId(0, (short) 0));
 *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
 *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
 *
 *   // Get some values out of the tree.
 *   tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 *   tree.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 *   tree.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 *   tree.get(new IntDataBox(3)); // Optional.empty();
 *
 *   // Iterate over the record ids in the tree. 遍历 eq scanAll range
 *   tree.scanEqual(new IntDataBox(2));         // [(2, 2)]
 *   tree.scanAll();                            // [(0, 0), (1, 1), (2, 2)]
 *   tree.scanGreaterEqual(new IntDataBox(1));  // [(1, 1), (2, 2)]
 *
 *   // Remove some elements from the tree.
 *   tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 *   tree.remove(new IntDataBox(0));
 *   tree.get(new IntDataBox(0)); // Optional.empty()
 *
 *   // Load the tree (same as creating a new tree).  从磁盘中加载树
 *   BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, lockContext);
 *
 *   // All the values are still there.
 *   fromDisk.get(new IntDataBox(0)); // Optional.empty()
 *   fromDisk.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 *   fromDisk.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 */
public class BPlusTree {
    // 缓冲区管理器
    private BufferManager bufferManager;

    // B+树元数据
    private BPlusTreeMetadata metadata;

    // B+树的根
    private BPlusNode root;

    // B+树的锁上下文
    private LockContext lockContext;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * 使用元数据`metadata`和锁上下文`lockContext`构造新的B+树。
     * `metadata`包含有关阶数、分区号、根页面号和键类型的信息。
     *
     * 如果指定的阶数太大以至于单个节点无法适应单个页面，则抛出BPlusTree异常。
     * 如果您希望具有最大填充的B+树节点，则使用BPlusTree.maxOrder函数获取适当的阶数。
     *
     * 我们还在_metadata.indices表中写入一行关于B+树的元数据：
     *
     *   - 树的名称（与其关联的表和索引的列）
     *   - 树的键模式，
     *   - 树的阶数，
     *   - 树的分区号，
     *   - 树根的页号。
     *
     * 在给定分区上分配的所有页面都是内部节点和叶节点的序列化。
     */
    public BPlusTree(BufferManager bufferManager, BPlusTreeMetadata metadata, LockContext lockContext) {
        // 防止子锁 - 我们只锁定整个树。
        lockContext.disableChildLocks();
        // 默认情况下我们要读取整棵树
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // 合理性检查.
        if (metadata.getOrder() < 0) {
            String msg = String.format(
                    "You cannot construct a B+ tree with negative order %d.",
                    metadata.getOrder());
            throw new BPlusTreeException(msg);
        }

        int maxOrder = BPlusTree.maxOrder(BufferManager.EFFECTIVE_PAGE_SIZE, metadata.getKeySchema());
        if (metadata.getOrder() > maxOrder) {
            String msg = String.format(
                    "You cannot construct a B+ tree with order %d greater than the " +
                            "max order %d.",
                    metadata.getOrder(), maxOrder);
            throw new BPlusTreeException(msg);
        }

        this.bufferManager = bufferManager;
        this.lockContext = lockContext;
        this.metadata = metadata;

        if (this.metadata.getRootPageNum() != DiskSpaceManager.INVALID_PAGE_NUM) {
            this.root = BPlusNode.fromBytes(this.metadata, bufferManager, lockContext,
                    this.metadata.getRootPageNum());
        } else {
            // 我们正在创建根节点，这意味着我们需要对树进行独占访问
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);
            // 构造根节点。
            List<DataBox> keys = new ArrayList<>();
            List<RecordId> rids = new ArrayList<>();
            Optional<Long> rightSibling = Optional.empty();
            this.updateRoot(new LeafNode(this.metadata, bufferManager, keys, rids, rightSibling, lockContext));
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    /**
     * 返回与`key`关联的值。 一对一给searchkey对应的值
     *
     *   // Insert a single value into the tree.
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(0, (short) 0);
     *   tree.put(key, rid);
     *
     *   // Get the value we put and also try to get a value we never put.
     *   tree.get(key);                 // Optional.of(rid)
     *   tree.get(new IntDataBox(100)); // Optional.empty()
     */
    public Optional<RecordId> get(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // 1.获取对应的叶子节点
        LeafNode leafNode = root.get(key);
        // 2.查询叶子节点
        List<DataBox> keys = leafNode.getKeys();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).equals(key)) {
                return Optional.of(leafNode.getRids().get(i));
            }
        }
        // 3.没有就返回空
        return Optional.empty();
    }

    /**
     * scanEqual(k)等价于get(k)，但它返回迭代器而不是Optional。
     * 也就是说，如果get(k)返回Optional.empty()，则scanEqual(k)返回空迭代器。
     * 如果get(k)为某个rid返回Optional.of(rid)，则scanEqual(k)返回rid上的迭代器。
     */
    public Iterator<RecordId> scanEqual(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        Optional<RecordId> rid = get(key);
        if (rid.isPresent()) {
            ArrayList<RecordId> l = new ArrayList<>();
            l.add(rid.get());
            return l.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    /**
     * 返回B+树中存储的所有RecordIds的迭代器，按其对应键的升序排列。
     *
     *   // Create a B+ tree and insert some values into it.
     *   BPlusTree tree = new BPlusTree("t.txt", Type.intType(), 4);
     *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     *   tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     *   tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     *   tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     *
     *   Iterator<RecordId> iter = tree.scanAll();
     *   iter.next(); // RecordId(1, 1)
     *   iter.next(); // RecordId(2, 2)
     *   iter.next(); // RecordId(3, 3)
     *   iter.next(); // RecordId(4, 4)
     *   iter.next(); // RecordId(5, 5)
     *   iter.next(); // NoSuchElementException
     *
     * 注意您不能将所有记录ID物化到内存中然后返回它们的迭代器。
     * 您的迭代器必须惰性地扫描B+树的叶节点。
     * 物化所有记录ID到内存中的解决方案将得分为0。
     */
    public Iterator<RecordId> scanAll() {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // TODO(proj2): Return a BPlusTreeIterator.
        return new BPlusTreeIterator();
    }

    /**
     * 返回B+树中存储的大于或等于`key`的所有RecordIds的迭代器。
     * RecordIds按其对应键的升序返回。
     *
     *   // Insert some values into a tree.
     *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     *   tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     *   tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     *   tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     *
     *   Iterator<RecordId> iter = tree.scanGreaterEqual(new IntDataBox(3));
     *   iter.next(); // RecordId(3, 3)
     *   iter.next(); // RecordId(4, 4)
     *   iter.next(); // RecordId(5, 5)
     *   iter.next(); // NoSuchElementException
     *
     * 注意您不能将所有记录ID物化到内存中然后返回它们的迭代器。
     * 您的迭代器必须惰性地扫描B+树的叶节点。
     * 物化所有记录ID到内存中的解决方案将得分为0。
     */
    public Iterator<RecordId> scanGreaterEqual(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // TODO(proj2): Return a BPlusTreeIterator.
        return new BPlusTreeIterator(key);
    }

    /**
     * 将(key, rid)对插入B+树。如果键已存在于B+树中，则不插入该对并引发异常。
     *
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(42, (short) 42);
     *   tree.put(key, rid); // Success :)
     *   tree.put(key, rid); // BPlusTreeException :(
     */
    public void put(DataBox key, RecordId rid) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);

        // TODO(proj2): implement
        // 注意：您不应直接更新根变量。
        // 如果旧根节点分裂，请使用提供的updateRoot()辅助方法来更改树的根节点。
        // 1.调用根节点的put方法
        Optional<Pair<DataBox, Long>> optional = root.put(key, rid);
        // 2.判断是否有Move UP
        if (optional.isPresent()) {
            // 3.有Move UP, 直接创建新的root节点
            Pair<DataBox, Long> pair = optional.get();
            DataBox newKey = pair.getFirst();
            long leftPageNum = root.getPage().getPageNum(); // 原来的root
            Long rightPageNum = pair.getSecond();           // 新分裂出的节点
            // 4.初始化数据
            List<DataBox> keys = new ArrayList<>();
            List<Long> children = new ArrayList<>();
            keys.add(newKey);
            children.add(leftPageNum);
            children.add(rightPageNum);
            // 5.创建新的根节点
            InnerNode node = new InnerNode(metadata, bufferManager, keys, children, lockContext);
            // 6.更新root
            updateRoot(node);
        }
    }

    /**
     * 将数据批量加载到B+树中。树应该为空，数据迭代器应该按排序顺序
     *（按DataBox键字段）并且不包含重复项（不对此进行错误检查）。
     *
     * fillFactor仅指定叶节点的填充因子；内部节点应该填充满并像在put中一样精确地拆分一半。
     *
     * 如果在批量加载时树不为空，此方法应引发异常。
     * 批量加载用于创建新索引，因此请考虑"空"树应该是什么样子。
     * 如果数据不满足前提条件（包含重复项或无序），则结果行为未定义。
     * 未定义行为意味着您可以随意处理这些情况（或者根本不处理），
     * 并且您不需要编写任何显式检查。
     *
     * 此方法的行为应类似于InnerNode的bulkLoad（参见BPlusNode.bulkLoad中的注释）。
     */
    public void bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor) {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);

        // TODO(proj2): implement
        // 注意：您不应直接更新根变量。
        // 如果旧根节点分裂，请使用提供的updateRoot()辅助方法来更改树的根节点。
        while (data.hasNext()) {
            // 1. 直接调用root的批量插入
            Optional<Pair<DataBox, Long>> optional = root.bulkLoad(data, fillFactor);
            // 2. 判断是否有up
            if (optional.isPresent()) {
                // 3. 由于这是来自root的up, 直接创建新的页面来承载就行
                Pair<DataBox, Long> pair = optional.get();

                DataBox newKey = pair.getFirst();
                long leftPageNum = root.getPage().getPageNum();
                Long rightPageNum = pair.getSecond();
                List<DataBox> keys = new ArrayList<>();
                keys.add(newKey);
                List<Long> children = new ArrayList<>();
                children.add(leftPageNum);
                children.add(rightPageNum);

                InnerNode node = new InnerNode(metadata, bufferManager, keys, children, lockContext);
                updateRoot(node);
            }
        }
    }

    /**
     * 从B+树中删除(key, rid)对。
     *
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(42, (short) 42);
     *
     *   tree.put(key, rid);
     *   tree.get(key); // Optional.of(rid)
     *   tree.remove(key);
     *   tree.get(key); // Optional.empty()
     */
    public void remove(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);

        // TODO(proj2): implement
        // 调用root的remove即可
        root.remove(key);
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * 返回此树的sexp表示。有关更多信息，请参见BPlusNode.toSexp。
     */
    public String toSexp() {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);
        return root.toSexp();
    }

    /**
     * 调试大型B+树很困难。为了使其更容易一些，我们可以将B+树打印为DOT文件，
     * 然后将其转换为B+树的漂亮图片。tree.toDot()返回说明B+树的DOT文件的内容。
     * 文件本身的细节并不重要，只需知道如果您调用tree.toDot()并将输出保存到名为tree.dot的文件中，
     * 然后您可以运行此命令
     *
     *   dot -T pdf tree.dot -o tree.pdf
     *
     * 来创建树的PDF。
     */
    public String toDot() {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        List<String> strings = new ArrayList<>();
        strings.add("digraph g {" );
        strings.add("  node [shape=record, height=0.1];");
        strings.add(root.toDot());
        strings.add("}");
        return String.join("\n", strings);
    }

    /**
     * 此函数与toDot()非常相似，只不过我们将B+树的点表示写入点文件，
     * 然后将其转换为将存储在src目录中的PDF。
     * 传入一个以".pdf"扩展名结尾的字符串（例如"tree.pdf"）。
     */
    public void toDotPDFFile(String filename) {
        String tree_string = toDot();

        // Writing to intermediate dot file
        try {
            java.io.File file = new java.io.File("tree.dot");
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(tree_string);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Running command to convert dot file to PDF
        try {
            Runtime.getRuntime().exec("dot -T pdf tree.dot -o " + filename).waitFor();
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new BPlusTreeException(e.getMessage());
        }
    }

    public BPlusTreeMetadata getMetadata() {
        return this.metadata;
    }

    /**
     * 返回最大数字d，使得具有2d个条目的LeafNode和具有2d个键的InnerNode将适合单个页面。
     */
    public static int maxOrder(short pageSize, Type keySchema) {
        int leafOrder = LeafNode.maxOrder(pageSize, keySchema);
        int innerOrder = InnerNode.maxOrder(pageSize, keySchema);
        return Math.min(leafOrder, innerOrder);
    }

    /** 返回B+树所在的分区号。 */
    public int getPartNum() {
        return metadata.getPartNum();
    }

    /**
     * 保存新的根页面号并更新树的元数据。
     **/
    private void updateRoot(BPlusNode newRoot) {
        this.root = newRoot;

        metadata.setRootPageNum(this.root.getPage().getPageNum());
        metadata.incrementHeight();
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction != null) {
            transaction.updateIndexMetadata(metadata);
        }
    }

    private void typecheck(DataBox key) {
        Type t = metadata.getKeySchema();
        if (!key.type().equals(t)) {
            String msg = String.format("DataBox %s is not of type %s", key, t);
            throw new IllegalArgumentException(msg);
        }
    }

    // Iterator ////////////////////////////////////////////////////////////////
    /**
     * 迭代器，用于迭代B+树的记录 RecordId
     * */
    private class BPlusTreeIterator implements Iterator<RecordId> {
        private LeafNode curLeaf;
        private int index;
        private List<DataBox> keys; // 缓存 keys
        private List<RecordId> rids; // 缓存 rids

        public BPlusTreeIterator() {
            curLeaf = root.getLeftmostLeaf();
            keys = curLeaf.getKeys();
            rids = curLeaf.getRids();
            index = 0;
        }

        public BPlusTreeIterator(DataBox key) {
            curLeaf = root.get(key);
            keys = curLeaf.getKeys();
            rids = curLeaf.getRids();

            // 找到第一个 >= key 的位置, 找不到就是size
            int pos = 0;
            for (pos = 0; pos < keys.size(); pos++) {
                if (key.compareTo(keys.get(pos)) <= 0) {
                    break;
                }
            }
            index = pos;
        }

        @Override
        public boolean hasNext() {
            return curLeaf.getRightSibling().isPresent() || index < rids.size();
        }

        @Override
        public RecordId next() {
            if (index < rids.size()) {
                return rids.get(index++);
            } else {
                if (curLeaf.getRightSibling().isPresent()) {
                    curLeaf = curLeaf.getRightSibling().get();
                    keys = curLeaf.getKeys();
                    rids = curLeaf.getRids();
                    index = 0;

                    // 处理空节点：跳过直到找到非空节点
                    while (rids.isEmpty()) {
                        if (!curLeaf.getRightSibling().isPresent()) {
                            throw new NoSuchElementException();
                        }
                        curLeaf = curLeaf.getRightSibling().get();
                        keys = curLeaf.getKeys();
                        rids = curLeaf.getRids();
                    }
                    return rids.get(index++);
                }


                throw new NoSuchElementException();
            }
        }

    }

}