package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterable;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.ConcatBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.IndexBacktrackingIterator;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.MasterLogRecord;

import java.util.*;

/**
 * LogManager 负责与日志本身进行交互。日志存储在自己的分区（分区 0）中。
 * 由于日志页面从不被删除，页号总是递增的，因此我们按以下方式分配 LSN：
 * - 页 1: [ LSN 10000, LSN 10040, LSN 10080, ...]
 * - 页 2: [ LSN 20000, LSN 20030, LSN 20055, ...]
 * - 页 3: [ LSN 30000, LSN 30047, LSN 30090, ...]
 * 每页最多允许 10,000 条日志记录。索引（后 4 位数字）是日志记录开始的页面内偏移量。
 * 日志记录不是固定宽度的，因此反向迭代不像正向迭代那样简单。
 * 第 0 页保留用于主记录，其中只包含几个日志条目：LSN 为 0 的主记录，
 * 后跟一个空的开始检查点记录和结束检查点记录。主记录是整个日志中唯一可以重写的记录。
 *
 * LogManager 还负责将 pageLSN 写入页面，并在页面被刷新时刷新日志，
 * 因此有几个方法必须在页面被获取和驱逐时由缓冲管理器调用
 * （fetchPageHook、fetchNewPageHook 和 pageEvictHook）。
 * 这些方法必须从缓冲管理器调用，以确保 pageLSN 是最新的，并且
 * flushedLSN >= 磁盘上的任何 pageLSN。
 */
public class LogManager implements Iterable<LogRecord>, AutoCloseable {
    private BufferManager bufferManager;    // 缓冲管理器
    private Deque<Page> unflushedLogTail;   // 未刷新的日志页
    private Page logTail;                   // 内存中的日志尾
    private Buffer logTailBuffer;           // 日志尾缓冲
    private boolean logTailPinned = false;  // 日志尾页是否被固定
    private long flushedLSN;                // 已刷新的LSN

    public static final int LOG_PARTITION = 0;

    LogManager(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
        this.unflushedLogTail = new ArrayDeque<>();

        // 创建日志尾
        // 注意这里调用的是NewPage,
        this.logTail = bufferManager.fetchNewPage(new DummyLockContext("_dummyLogPageRecord"), LOG_PARTITION);
        this.unflushedLogTail.add(this.logTail);
        this.logTailBuffer = this.logTail.getBuffer();
        this.logTail.unpin();

        this.flushedLSN = maxLSN(this.logTail.getPageNum() - 1L);
    }

    /**
     * 将日志的第一条记录重写。
     * @param record 用于替换第一条记录的日志记录
     */
    public synchronized void rewriteMasterRecord(MasterLogRecord record) {
        Page firstPage = bufferManager.fetchPage(new DummyLockContext("_dummyLogPageRecord"), LOG_PARTITION);
        try {
            firstPage.getBuffer().put(record.toBytes());
            firstPage.flush();
        } finally {
            firstPage.unpin();
        }
    }

    /**
     * 将一条日志记录追加到日志中。
     * @param record 要追加到日志中的日志记录
     * @return 新日志记录的LSN
     */
    public synchronized long appendToLog(LogRecord record) {
        byte[] bytes = record.toBytes();
        // 循环，以防访问日志尾部需要刷新日志以驱逐脏页来加载日志尾部
        do {
            // 如果日志尾部已满或者不够，则创建新的日志尾
            if (logTailBuffer == null || bytes.length > DiskSpaceManager.PAGE_SIZE - logTailBuffer.position()) {
                logTailPinned = true;
                logTail = bufferManager.fetchNewPage(new DummyLockContext("_dummyLogPageRecord"), LOG_PARTITION);
                unflushedLogTail.add(logTail);
                logTailBuffer = logTail.getBuffer();
            } else {
                logTailPinned = true;
                logTail.pin();
                if (logTailBuffer == null) {
                    logTail.unpin();
                }
            }
        } while (logTailBuffer == null);


        try {
            int pos = logTailBuffer.position();
            logTailBuffer.put(bytes);
            long LSN = makeLSN(unflushedLogTail.getLast().getPageNum(), pos);
            record.LSN = LSN;
            return LSN;
        } finally {
            logTail.unpin();
            logTailPinned = false;
        }
    }

    /**
     * 获取特定的日志记录。
     * @param LSN 要获取的记录的LSN
     * @return 具有指定LSN的日志记录
     */
    public LogRecord fetchLogRecord(long LSN) {
        try {
            Page logPage = bufferManager.fetchPage(new DummyLockContext("_dummyLogPageRecord"), getLSNPage(LSN));
            try {
                Buffer buf = logPage.getBuffer();
                buf.position(getLSNIndex(LSN));
                Optional<LogRecord> record = LogRecord.fromBytes(buf);
                record.ifPresent((LogRecord e) -> e.setLSN(LSN));
                return record.orElse(null);
            } finally {
                logPage.unpin();
            }
        } catch (PageException e) {
            return null;
        }
    }

    /**
     * 将日志刷新到至少指定的记录，
     * 实际上是刷新到包含由LSN指定的记录的页面。
     * @param LSN 日志应该刷新到的LSN
     */
    public synchronized void flushToLSN(long LSN) {
        Iterator<Page> iter = unflushedLogTail.iterator();
        long pageNum = getLSNPage(LSN);
        while (iter.hasNext()) {
            Page page = iter.next();
            if (page.getPageNum() > pageNum) {
                break;
            }
            page.flush();
            iter.remove();
        }
        flushedLSN = Math.max(flushedLSN, maxLSN(pageNum));
        if (unflushedLogTail.size() == 0) {
            if (!logTailPinned) {
                logTail = null;
            }
            logTailBuffer = null;
        }
    }

    /**
     * @return flushedLSN
     */
    public long getFlushedLSN() {
        return flushedLSN;
    }

    /**
     * 根据日志页号和索引生成LSN
     * @param pageNum 日志页的页号
     * @param index 日志页内日志记录的索引
     * @return LSN
     */
    static long makeLSN(long pageNum, int index) {
        return DiskSpaceManager.getPageNum(pageNum) * 10000L + index;
    }

    /**
     * 生成给定页上的最大可能LSN
     * @param pageNum 日志页的页号
     * @return 日志页上的最大可能LSN
     */
    static long maxLSN(long pageNum) {
        return makeLSN(pageNum, 9999);
    }

    /**
     * 获取对应LSN的记录所在的页号
     * @param LSN 要获取页号的LSN
     * @return LSN所在的页
     */
    static long getLSNPage(long LSN) {
        return LSN / 10000L;
    }

    /**
     * 获取对应LSN的记录在页内的索引
     * @param LSN 要获取索引的LSN
     * @return LSN所在的页内的索引
     */
    static int getLSNIndex(long LSN) {
        return (int) (LSN % 10000L);
    }

    /**
     * 从LSN开始向前扫描日志。
     * @param LSN 开始扫描的LSN
     * @return 从LSN开始的日志条目迭代器
     */
    public Iterator<LogRecord> scanFrom(long LSN) {
        return new ConcatBacktrackingIterator<>(new LogPagesIterator(LSN));
    }

    /**
     * 从第一条记录开始向前扫描日志。
     * @return 所有日志条目的迭代器
     */
    @Override
    public Iterator<LogRecord> iterator() {
        return this.scanFrom(0);
    }

    @Override
    public synchronized void close() {
        if (!this.unflushedLogTail.isEmpty()) {
            this.flushToLSN(maxLSN(unflushedLogTail.getLast().getPageNum()));
        }
    }

    private class LogPageIterator extends IndexBacktrackingIterator<LogRecord> {
        private Page logPage;
        private int startIndex;

        private LogPageIterator(Page logPage, int startIndex) {
            super(DiskSpaceManager.PAGE_SIZE);
            this.logPage = logPage;
            this.startIndex = startIndex;
            this.logPage.unpin();
        }

        @Override
        protected int getNextNonEmpty(int currentIndex) {
            logPage.pin();
            try {
                Buffer buf = logPage.getBuffer();
                if (currentIndex == -1) {
                    currentIndex = startIndex;
                    buf.position(currentIndex);
                } else {
                    buf.position(currentIndex);
                    LogRecord.fromBytes(buf);
                    currentIndex = buf.position();
                }

                if (LogRecord.fromBytes(buf).isPresent()) {
                    return currentIndex;
                } else {
                    return DiskSpaceManager.PAGE_SIZE;
                }
            } finally {
                logPage.unpin();
            }
        }

        @Override
        protected LogRecord getValue(int index) {
            logPage.pin();
            try {
                Buffer buf = logPage.getBuffer();
                buf.position(index);
                LogRecord record = LogRecord.fromBytes(buf).orElseThrow(NoSuchElementException::new);
                record.setLSN(makeLSN(logPage.getPageNum(), index));
                return record;
            } finally {
                logPage.unpin();
            }
        }
    }

    private class LogPagesIterator implements BacktrackingIterator<BacktrackingIterable<LogRecord>> {
        private BacktrackingIterator<LogRecord> nextIter;
        private long nextIndex;

        private LogPagesIterator(long startLSN) {
            nextIndex = getLSNPage(startLSN);
            try {
                Page page = bufferManager.fetchPage(new DummyLockContext(), nextIndex);
                nextIter = new LogPageIterator(page, getLSNIndex(startLSN));
            } catch (PageException e) {
                nextIter = null;
            }
        }

        @Override
        public void markPrev() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markNext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            return nextIter != null;
        }

        @Override
        public BacktrackingIterable<LogRecord> next() {
            if (hasNext()) {
                final BacktrackingIterator<LogRecord> iter = nextIter;
                BacktrackingIterable<LogRecord> iterable = () -> iter;

                nextIter = null;
                do {
                    ++nextIndex;
                    try {
                        Page page = bufferManager.fetchPage(new DummyLockContext(), nextIndex);
                        nextIter = new LogPageIterator(page, 0);
                    } catch (PageException e) {
                        break;
                    }
                } while (!nextIter.hasNext());

                return iterable;
            }
            throw new NoSuchElementException();
        }
    }
}
