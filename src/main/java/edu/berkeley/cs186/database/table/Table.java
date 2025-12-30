package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.Bits;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterable;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.ConcatBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.IndexBacktrackingIterator;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * # 概述
 * Table 表示一个数据库表，用户可以对表进行插入、获取、更新和删除记录操作：
 *
 *   // 创建一个全新的表 t(x: int, y: int)，该表持久化在与 `pageDirectory` 关联的堆文件中。
 *   List<String> fieldNames = Arrays.asList("x", "y");
 *   List<String> fieldTypes = Arrays.asList(Type.intType(), Type.intType());
 *   Schema schema = new Schema(fieldNames, fieldTypes);
 *   Table t = new Table("t", schema, pageDirectory, new DummyLockContext());
 *
 *   // 插入、获取、更新和删除记录。
 *   List<DataBox> a = Arrays.asList(new IntDataBox(1), new IntDataBox(2));
 *   List<DataBox> b = Arrays.asList(new IntDataBox(3), new IntDataBox(4));
 *   RecordId rid = t.addRecord(a);
 *   Record ra = t.getRecord(rid);
 *   t.updateRecord(b, rid);
 *   Record rb = t.getRecord(rid);
 *   t.deleteRecord(rid);
 *
 * # 持久化
 * 每个表都持久化在它自己的 PageDirectory 对象中（在构造函数中传入），
 * 该对象与 BufferManager 和 DiskSpaceManager 交互以将表保存到磁盘。
 *
 * 可以通过使用相同的参数构造表来重新加载该表。
 *
 * # 存储格式
 * 现在，我们讨论表如何序列化它们的数据。
 *
 * 所有页面都是数据页——没有头页面，因为所有元数据都存储在其他地方
 * （作为 _metadata.tables 表中的行）。每个数据页以 n 字节的位图开始，
 * 后面跟着 m 条记录。位图指示哪些记录在页面中是有效的。
 * n 和 m 的值被设置为最大化每页的记录数（详情请参见 computeDataPageNumbers）。
 *
 * 例如，如果我们有 5 字节的页面和 1 字节的记录，那么表的文件看起来就像这样：
 *
 *          +----------+----------+----------+----------+----------+ \
 *   Page 0 | 1001xxxx | 01111010 | xxxxxxxx | xxxxxxxx | 01100001 |  |
 *          +----------+----------+----------+----------+----------+  |
 *   Page 1 | 1101xxxx | 01110010 | 01100100 | xxxxxxxx | 01101111 |  |- 数据
 *          +----------+----------+----------+----------+----------+  |
 *   Page 2 | 0011xxxx | xxxxxxxx | xxxxxxxx | 01111010 | 00100001 |  |
 *          +----------+----------+----------+----------+----------+ /
 *           \________/ \________/ \________/ \________/ \________/
 *            位图       记录 0     记录 1     记录 2     记录 3
 *
 *  - 第一页（Page 0）是一个数据页。这个数据页的第一个字节是位图，
 *    接下来的四个字节分别是记录。第一位和第四位被设置，表示记录 0 和记录 3 是有效的。
 *    记录 1 和记录 2 是无效的，所以我们忽略它们的内容。
 *    同样，位图的最后四位未使用，所以我们忽略它们的内容。
 *  - 第二页和第三页（Page 1 和 2）也是数据页，格式与第 0 页类似。
 *
 *  当我们向表中添加记录时，我们会将其添加到表中的第一个空闲槽中。
 *  有关更多信息，请参见 addRecord。
 *
 * 有些表有大记录。为了有效地处理有大记录的表（仍然可以适合一个页面），
 * 我们以稍微不同的方式格式化这些表，给每个记录一个完整的页面。
 * 全页记录的表没有位图。相反，每个分配的页面是一个单独的记录，
 * 我们通过简单地释放页面来表示页面不包含记录。
 *
 * 在某些情况下，即使对于小记录，这种行为也可能是可取的
 * （我们的数据库只支持页面级锁定，所以在每个元组的 I/O 成本下需要元组级锁的情况下，全页记录可能是可取的）,
 * 并且可以通过 setFullPageRecords 方法显式切换。
 */
public class Table implements BacktrackingIterable<Record> {
    // The name of the table.
    private String name;

    // The schema of the table.
    private Schema schema;

    // The page directory persisting the table.
    private PageDirectory pageDirectory;

    // The size (in bytes) of the bitmap found at the beginning of each data page.
    private int bitmapSizeInBytes;

    // The number of records on each data page.
    private int numRecordsPerPage;

    // The lock context of the table.
    private LockContext tableContext;

    // Statistics about the contents of the database.
    Map<String, TableStats> stats;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Load a table named `name` with schema `schema` from `pageDirectory`. `lockContext`
     * is the lock context of the table (use a DummyLockContext() to disable locking). A
     * new table will be created if none exists in the pageDirectory.
     */
    public Table(String name, Schema schema, PageDirectory pageDirectory, LockContext lockContext, Map<String, TableStats> stats) {
        this.name = name;
        this.pageDirectory = pageDirectory;
        this.schema = schema;
        this.tableContext = lockContext;

        this.bitmapSizeInBytes = computeBitmapSizeInBytes(pageDirectory.getEffectivePageSize(), schema);
        this.numRecordsPerPage = computeNumRecordsPerPage(pageDirectory.getEffectivePageSize(), schema);
        // mark everything that is not used for records as metadata
        this.pageDirectory.setEmptyPageMetadataSize((short) (pageDirectory.getEffectivePageSize() - numRecordsPerPage
                                               * schema.getSizeInBytes()));
        this.stats = stats;
        if (!this.stats.containsKey(name)) this.stats.put(name, new TableStats(this.schema, this.numRecordsPerPage));
    }

    public Table(String name, Schema schema, PageDirectory pageDirectory, LockContext lockContext) {
        this(name, schema, pageDirectory, lockContext, new HashMap<>());
    }
    // Accessors ///////////////////////////////////////////////////////////////
    public String getName() {
        return name;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getNumRecordsPerPage() {
        return numRecordsPerPage;
    }

    public void setFullPageRecords() {
        numRecordsPerPage = 1;
        bitmapSizeInBytes = 0;
        pageDirectory.setEmptyPageMetadataSize((short) (pageDirectory.getEffectivePageSize() -
                                          schema.getSizeInBytes()));
    }

    public TableStats getStats() {
        return this.stats.get(name);
    }

    public int getNumDataPages() {
        return this.pageDirectory.getNumDataPages();
    }

    public int getPartNum() {
        return pageDirectory.getPartNum();
    }

    private byte[] getBitMap(Page page) {
        if (bitmapSizeInBytes > 0) {
            byte[] bytes = new byte[bitmapSizeInBytes];
            page.getBuffer().get(bytes, 0, bitmapSizeInBytes);
            return bytes;
        } else {
            return new byte[] {(byte) 0xFF};
        }
    }

    private void writeBitMap(Page page, byte[] bitmap) {
        if (bitmapSizeInBytes > 0) {
            assert bitmap.length == bitmapSizeInBytes;
            page.getBuffer().put(bitmap, 0, bitmapSizeInBytes);
        }
    }

    private static int computeBitmapSizeInBytes(int pageSize, Schema schema) {
        int recordsPerPage = computeNumRecordsPerPage(pageSize, schema);
        if (recordsPerPage == 1) return 0;
        if (recordsPerPage % 8 == 0) return recordsPerPage / 8;
        return recordsPerPage / 8 + 1;
    }

    /**
     * Computes the maximum number of records per page of the given `schema` that
     * can fit on a page with `pageSize` bytes of space, including the overhead
     * for the bitmap. In most cases this can be computed as the number of bits
     * in the page floor divided by the number of bits per record. In the
     * special case where only a single record can fit in a page, no bitmap is
     * needed.
     * @param pageSize size of page in bytes
     * @param schema schema for the records to be stored on this page
     * @return the maximum number of records that can be stored per page
     */
    public static int computeNumRecordsPerPage(int pageSize, Schema schema) {
        int schemaSize = schema.getSizeInBytes();
        if (schemaSize > pageSize) {
            throw new DatabaseException(String.format(
                    "Schema of size %f bytes is larger than effective page size",
                    schemaSize
            ));
        }
        if (2 * schemaSize + 1 > pageSize) {
            // special case: full page records with no bitmap. Checks if two
            // records + bitmap is larger than the effective page size
            return 1;
        }
        // +1 for space in bitmap
        int recordOverheadInBits = 1 + 8 * schema.getSizeInBytes();
        int pageSizeInBits = pageSize  * 8;
        return pageSizeInBits / recordOverheadInBits;
    }

    // Modifiers ///////////////////////////////////////////////////////////////
    /**
     * buildStatistics builds histograms on each of the columns of a table. Running
     * it multiple times refreshes the statistics
     */
    public void buildStatistics(int buckets) {
        this.stats.get(name).refreshHistograms(buckets, this);
    }

    private synchronized void insertRecord(Page page, int entryNum, Record record) {
        int offset = bitmapSizeInBytes + (entryNum * schema.getSizeInBytes());
        page.getBuffer().position(offset).put(record.toBytes(schema));
    }

    /**
     * addRecord adds a record to this table and returns the record id of the
     * newly added record. stats, freePageNums, and numRecords are updated
     * accordingly. The record is added to the first free slot of the first free
     * page (if one exists, otherwise one is allocated). For example, if the
     * first free page has bitmap 0b11101000, then the record is inserted into
     * the page with index 3 and the bitmap is updated to 0b11111000.
     */
    public synchronized RecordId addRecord(Record record) {
        record = schema.verify(record);
        Page page = pageDirectory.getPageWithSpace(schema.getSizeInBytes());
        try {
            // Find the first empty slot in the bitmap.
            // entry number of the first free slot and store it in entryNum; and (2) we
            // count the total number of entries on this page.
            byte[] bitmap = getBitMap(page);
            int entryNum = 0;
            for (; entryNum < numRecordsPerPage; ++entryNum) {
                if (Bits.getBit(bitmap, entryNum) == Bits.Bit.ZERO) {
                    break;
                }
            }
            if (numRecordsPerPage == 1) {
                entryNum = 0;
            }
            assert (entryNum < numRecordsPerPage);

            // Insert the record and update the bitmap.
            insertRecord(page, entryNum, record);
            Bits.setBit(bitmap, entryNum, Bits.Bit.ONE);
            writeBitMap(page, bitmap);

            // Update the metadata.
            stats.get(name).addRecord(record);
            return new RecordId(page.getPageNum(), (short) entryNum);
        } finally {
            page.unpin();
        }
    }

    /**
     * Retrieves a record from the table, throwing an exception if no such record
     * exists.
     */
    public synchronized Record getRecord(RecordId rid) {
        validateRecordId(rid);
        Page page = fetchPage(rid.getPageNum());
        try {
            byte[] bitmap = getBitMap(page);
            if (Bits.getBit(bitmap, rid.getEntryNum()) == Bits.Bit.ZERO) {
                String msg = String.format("Record %s does not exist.", rid);
                throw new DatabaseException(msg);
            }

            int offset = bitmapSizeInBytes + (rid.getEntryNum() * schema.getSizeInBytes());
            Buffer buf = page.getBuffer();
            buf.position(offset);
            return Record.fromBytes(buf, schema);
        } finally {
            page.unpin();
        }
    }

    /**
     * 使用新值覆盖现有记录并返回被覆盖的记录。统计信息会相应更新。如果 rid 不对应表中现有的记录，
     * 则抛出异常。
     */
    public synchronized Record updateRecord(RecordId rid, Record updated) {
        validateRecordId(rid);
        // 如果我们要更新记录，需要对所在页面有独占访问权限。
        LockContext pageContext = tableContext.childContext(rid.getPageNum());
        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(pageContext, LockType.X);

        Record newRecord = schema.verify(updated);
        Record oldRecord = getRecord(rid);

        Page page = fetchPage(rid.getPageNum());
        try {
            insertRecord(page, rid.getEntryNum(), newRecord);

            this.stats.get(name).removeRecord(oldRecord);
            this.stats.get(name).addRecord(newRecord);
            return oldRecord;
        } finally {
            page.unpin();
        }
    }

    /**
     * 从表中删除并返回由 rid 指定的记录，并相应地更新统计信息、空闲页面编号和记录数量。
     * 如果 rid 不对应表中现有的记录，则抛出异常。
     */
    public synchronized Record deleteRecord(RecordId rid) {
        validateRecordId(rid);
        LockContext pageContext = tableContext.childContext(rid.getPageNum());

        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(pageContext, LockType.X);

        Page page = fetchPage(rid.getPageNum());
        try {
            Record record = getRecord(rid);

            byte[] bitmap = getBitMap(page);
            Bits.setBit(bitmap, rid.getEntryNum(), Bits.Bit.ZERO);
            writeBitMap(page, bitmap);

            stats.get(name).removeRecord(record);
            int numRecords = numRecordsPerPage == 1 ? 0 : numRecordsOnPage(page);
            pageDirectory.updateFreeSpace(page,
                                     (short) ((numRecordsPerPage - numRecords) * schema.getSizeInBytes()));
            return record;
        } finally {
            page.unpin();
        }
    }

    @Override
    public String toString() {
        return "Table " + name;
    }

    // Helpers /////////////////////////////////////////////////////////////////
    private Page fetchPage(long pageNum) {
        try {
            return pageDirectory.getPage(pageNum);
        } catch (PageException e) {
            throw new DatabaseException(e);
        }
    }

    private int numRecordsOnPage(Page page) {
        byte[] bitmap = getBitMap(page);
        int numRecords = 0;
        for (int i = 0; i < numRecordsPerPage; ++i) {
            if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                numRecords++;
            }
        }
        return numRecords;
    }

    private void validateRecordId(RecordId rid) {
        int e = rid.getEntryNum();

        if (e < 0) {
            String msg = String.format("Invalid negative entry number %d.", e);
            throw new DatabaseException(msg);
        }

        if (e >= numRecordsPerPage) {
            String msg = String.format(
                             "There are only %d records per page, but record %d was requested.",
                             numRecordsPerPage, e);
            throw new DatabaseException(msg);
        }
    }

    // Iterators ///////////////////////////////////////////////////////////////

    /**
     * @return 对表进行全表扫描，返回所有现有记录的ID
     */
    public BacktrackingIterator<RecordId> ridIterator() {
        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);

        BacktrackingIterator<Page> iter = pageDirectory.iterator();
        return new ConcatBacktrackingIterator<>(new PageIterator(iter, false));
    }

    /**
     * @param rids 一个包含此表中记录ID的迭代器
     * @return 一个遍历对应记录ID的记录的迭代器。如果记录ID迭代器支持回溯，则新的记录迭代器也支持回溯。
     */
    public BacktrackingIterator<Record> recordIterator(Iterator<RecordId> rids) {
        // TODO(proj4_part2): Update the following line
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        return new RecordIterator(rids);
    }

    public BacktrackingIterator<Page> pageIterator() {
        return pageDirectory.iterator();
    }

    @Override
    public BacktrackingIterator<Record> iterator() {
        // returns an iterator over all the records in this table
        return new RecordIterator(ridIterator());
    }

    /**
     * RIDPageIterator is a BacktrackingIterator over the RecordIds of a single
     * page of the table.
     *
     * See comments on the BacktrackingIterator interface for how mark and reset
     * should function.
     */
    class RIDPageIterator extends IndexBacktrackingIterator<RecordId> {
        private Page page;
        private byte[] bitmap;

        RIDPageIterator(Page page) {
            super(numRecordsPerPage);
            this.page = page;
            this.bitmap = getBitMap(page);
            page.unpin();
        }

        @Override
        protected int getNextNonEmpty(int currentIndex) {
            for (int i = currentIndex + 1; i < numRecordsPerPage; ++i) {
                if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                    return i;
                }
            }
            return numRecordsPerPage;
        }

        @Override
        protected RecordId getValue(int index) {
            return new RecordId(page.getPageNum(), (short) index);
        }
    }

    private class PageIterator implements BacktrackingIterator<BacktrackingIterable<RecordId>> {
        private BacktrackingIterator<Page> sourceIterator;
        private boolean pinOnFetch;

        private PageIterator(BacktrackingIterator<Page> sourceIterator, boolean pinOnFetch) {
            this.sourceIterator = sourceIterator;
            this.pinOnFetch = pinOnFetch;
        }

        @Override
        public void markPrev() {
            sourceIterator.markPrev();
        }

        @Override
        public void markNext() {
            sourceIterator.markNext();
        }

        @Override
        public void reset() {
            sourceIterator.reset();
        }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public BacktrackingIterable<RecordId> next() {
            return new InnerIterable(sourceIterator.next());
        }

        private class InnerIterable implements BacktrackingIterable<RecordId> {
            private Page baseObject;

            private InnerIterable(Page baseObject) {
                this.baseObject = baseObject;
                if (!pinOnFetch) {
                    baseObject.unpin();
                }
            }

            @Override
            public BacktrackingIterator<RecordId> iterator() {
                baseObject.pin();
                return new RIDPageIterator(baseObject);
            }
        }
    }

    /**
     * Wraps an iterator of record ids to form an iterator over records.
     */
    private class RecordIterator implements BacktrackingIterator<Record> {
        private Iterator<RecordId> ridIter;

        public RecordIterator(Iterator<RecordId> ridIter) {
            this.ridIter = ridIter;
        }

        @Override
        public boolean hasNext() {
            return ridIter.hasNext();
        }

        @Override
        public Record next() {
            try {
                return getRecord(ridIter.next());
            } catch (DatabaseException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void markPrev() {
            if (ridIter instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIter).markPrev();
            } else {
                throw new UnsupportedOperationException("Cannot markPrev using underlying iterator");
            }
        }

        @Override
        public void markNext() {
            if (ridIter instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIter).markNext();
            } else {
                throw new UnsupportedOperationException("Cannot markNext using underlying iterator");
            }
        }

        @Override
        public void reset() {
            if (ridIter instanceof BacktrackingIterator) {
                ((BacktrackingIterator<RecordId>) ridIter).reset();
            } else {
                throw new UnsupportedOperationException("Cannot reset using underlying iterator");
            }
        }
    }
}

