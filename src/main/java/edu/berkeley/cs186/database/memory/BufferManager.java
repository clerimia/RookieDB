package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.recovery.LogManager;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * 缓冲区管理器的实现，支持可配置的页面替换策略。
 * 数据存储在页面大小的字节数组中，并在返回时包装在特定于加载页面的Frame对象中
 * （驱逐页面并在框架中加载新页面将产生一个新的Frame对象，但底层字节数组相同），
 * 使用相同字节数组支持的旧Frame对象将被标记为无效。
 */
public class BufferManager implements AutoCloseable {
    // 我们在每个页面上保留36个字节用于恢复的簿记
    // （用于存储pageLSN，并确保仅重做/仅撤销的日志记录可以
    // 适合一页）。
    public static final short RESERVED_SPACE = 36;

    // 缓冲区管理器用户可用的有效页面大小。
    public static final short EFFECTIVE_PAGE_SIZE = (short) (DiskSpaceManager.PAGE_SIZE - RESERVED_SPACE);

    // 缓冲区帧数组
    private Frame[] frames;

    // 指向此缓冲区管理器实例下的磁盘空间管理器的引用。
    private DiskSpaceManager diskSpaceManager;

    // 页面号到帧索引的映射
    private Map<Long, Integer> pageToFrame;

    // 缓冲区管理器上的锁
    private ReentrantLock managerLock;

    // 驱逐策略
    private EvictionPolicy evictionPolicy;

    // 第一个空闲帧的索引
    private int firstFreeIndex;

    // 恢复管理器
    private RecoveryManager recoveryManager;

    // I/O操作计数
    private long numIOs = 0;

    /**
     * 缓冲帧，包含有关加载页面的信息，包装在底层字节数组周围。空闲帧使用索引字段在空闲帧之间创建（单向）链表。
     */
    class Frame extends BufferFrame {
        private static final int INVALID_INDEX = Integer.MIN_VALUE;

        byte[] contents;
        private int index;
        private long pageNum;
        private boolean dirty;
        private ReentrantLock frameLock;
        private boolean logPage;

        Frame(byte[] contents, int nextFree) {
            this(contents, ~nextFree, DiskSpaceManager.INVALID_PAGE_NUM);
        }

        Frame(Frame frame) {
            this(frame.contents, frame.index, frame.pageNum);
        }

        Frame(byte[] contents, int index, long pageNum) {
            this.contents = contents;
            this.index = index;
            this.pageNum = pageNum;
            this.dirty = false;
            this.frameLock = new ReentrantLock();
            int partNum = DiskSpaceManager.getPartNum(pageNum);
            this.logPage = partNum == LogManager.LOG_PARTITION;
        }

        /**
         * 锁定缓冲帧；锁定时无法被驱逐。当缓冲帧被锁定时会发生"命中"。
         */
        @Override
        public void pin() {
            this.frameLock.lock();

            if (!this.isValid()) {
                throw new IllegalStateException("pinning invalidated frame");
            }

            super.pin();
        }

        /**
         * 解锁缓冲帧。
         */
        @Override
        public void unpin() {
            super.unpin();
            this.frameLock.unlock();
        }

        /**
         * @return 此帧是否有效
         */
        @Override
        public boolean isValid() {
            return this.index >= 0;
        }

        /**
         * @return 此帧的页面是否已被释放
         */
        private boolean isFreed() {
            return this.index < 0 && this.index != INVALID_INDEX;
        }

        /**
         * 使帧无效，必要时刷新它。
         */
        private void invalidate() {
            if (this.isValid()) {
                this.flush();
            }
            this.index = INVALID_INDEX;
            this.contents = null;
        }

        /**
         * 将帧标记为空闲。
         */
        private void setFree() {
            if (isFreed()) {
                throw new IllegalStateException("cannot free free frame");
            }
            int nextFreeIndex = firstFreeIndex;
            firstFreeIndex = this.index;
            this.index = ~nextFreeIndex;
        }

        private void setUsed() {
            if (!isFreed()) {
                throw new IllegalStateException("cannot unfree used frame");
            }
            int index = firstFreeIndex;
            firstFreeIndex = ~this.index;
            this.index = index;
        }

        /**
         * @return 此帧的页号
         */
        @Override
        public long getPageNum() {
            return this.pageNum;
        }

        /**
         * 将此缓冲帧刷新到磁盘，但不卸载它。
         */
        @Override
        void flush() {
            this.frameLock.lock();
            super.pin();
            try {
                if (!this.isValid()) {
                    return;
                }
                if (!this.dirty) {
                    return;
                }
                if (!this.logPage) {
                    recoveryManager.pageFlushHook(this.getPageLSN());
                }
                BufferManager.this.diskSpaceManager.writePage(pageNum, contents);
                BufferManager.this.incrementIOs();
                this.dirty = false;
            } finally {
                super.unpin();
                this.frameLock.unlock();
            }
        }

        /**
         * 从缓冲帧读取。
         * @param position 缓冲帧中开始读取的位置
         * @param num 要读取的字节数
         * @param buf 输出缓冲区
         */
        @Override
        void readBytes(short position, short num, byte[] buf) {
            this.pin();
            try {
                if (!this.isValid()) {
                    throw new IllegalStateException("reading from invalid buffer frame");
                }
                System.arraycopy(this.contents, position + dataOffset(), buf, 0, num);
                BufferManager.this.evictionPolicy.hit(this);
            } finally {
                this.unpin();
            }
        }

        /**
         * 写入缓冲帧，并将帧标记为已修改。
         * @param position 缓冲帧中开始写入的位置
         * @param num 要写入的字节数
         * @param buf 输入缓冲区
         */
        @Override
        void writeBytes(short position, short num, byte[] buf) {
            this.pin();
            try {
                if (!this.isValid()) {
                    throw new IllegalStateException("writing to invalid buffer frame");
                }
                int offset = position + dataOffset();
                TransactionContext transaction = TransactionContext.getTransaction();
                if (transaction != null && !logPage) {
                    List<Pair<Integer, Integer>> changedRanges = getChangedBytes(offset, num, buf);
                    for (Pair<Integer, Integer> range : changedRanges) {
                        int start = range.getFirst();
                        int len = range.getSecond();
                        byte[] before = Arrays.copyOfRange(contents, start + offset, start + offset + len);
                        byte[] after = Arrays.copyOfRange(buf, start, start + len);
                        long pageLSN = recoveryManager.logPageWrite(transaction.getTransNum(), pageNum, (short) (start + position), before,
                                       after);
                        this.setPageLSN(pageLSN);
                    }
                }
                System.arraycopy(buf, 0, this.contents, offset, num);
                this.dirty = true;
                BufferManager.this.evictionPolicy.hit(this);
            } finally {
                this.unpin();
            }
        }

        /**
         * 请求页面的有效Frame对象（如果无效，则返回新的Frame对象）。
         * 页面在返回时被锁定。
         */
        @Override
        Frame requestValidFrame() {
            this.frameLock.lock();
            try {
                if (this.isFreed()) {
                    throw new PageException("page already freed");
                }
                if (this.isValid()) {
                    this.pin();
                    return this;
                }
                return BufferManager.this.fetchPageFrame(this.pageNum);
            } finally {
                this.frameLock.unlock();
            }
        }

        @Override
        short getEffectivePageSize() {
            if (logPage) {
                return DiskSpaceManager.PAGE_SIZE;
            } else {
                return BufferManager.EFFECTIVE_PAGE_SIZE;
            }
        }

        @Override
        long getPageLSN() {
            return ByteBuffer.wrap(this.contents).getLong(8);
        }

        @Override
        public String toString() {
            if (index >= 0) {
                return "Buffer Frame " + index + ", Page " + pageNum + (isPinned() ? " (pinned)" : "");
            } else if (index == INVALID_INDEX) {
                return "Buffer Frame (evicted), Page " + pageNum;
            } else {
                return "Buffer Frame (freed), next free = " + (~index);
            }
        }

        /**
         * 生成buf与contents不同的位置的（偏移量，长度）对。合并附近的对
         * （其中附近定义为对之间有少于BufferManager.RESERVED_SPACE字节的未修改数据）。
         */
        private List<Pair<Integer, Integer>> getChangedBytes(int offset, int num, byte[] buf) {
            List<Pair<Integer, Integer>> ranges = new ArrayList<>();
            int maxRange = EFFECTIVE_PAGE_SIZE / 2;
            int startIndex = -1;
            int skip = -1;
            for (int i = 0; i < num; ++i) {
                if (startIndex >= 0 && maxRange == i - startIndex) {
                    ranges.add(new Pair<>(startIndex, maxRange));
                    startIndex = -1;
                    skip = -1;
                } else if (buf[i] == contents[offset + i] && startIndex >= 0) {
                    if (skip > BufferManager.RESERVED_SPACE) {
                        ranges.add(new Pair<>(startIndex, i - startIndex - skip));
                        startIndex = -1;
                        skip = -1;
                    } else {
                        ++skip;
                    }
                } else if (buf[i] != contents[offset + i]) {
                    if (startIndex < 0) {
                        startIndex = i;
                    }
                    skip = 0;
                }
            }
            if (startIndex >= 0) {
                ranges.add(new Pair<>(startIndex, num - startIndex - skip));
            }
            return ranges;
        }

        void setPageLSN(long pageLSN) {
            ByteBuffer.wrap(this.contents).putLong(8, pageLSN);
        }

        private short dataOffset() {
            if (logPage) {
                return 0;
            } else {
                return BufferManager.RESERVED_SPACE;
            }
        }
    }

    /**
     * 创建新的缓冲管理器。
     *
     * @param diskSpaceManager 底层磁盘空间管理器
     * @param bufferSize 缓冲区大小（以页为单位）
     * @param evictionPolicy 要使用的驱逐策略
     */
    public BufferManager(DiskSpaceManager diskSpaceManager, RecoveryManager recoveryManager,
                         int bufferSize, EvictionPolicy evictionPolicy) {
        this.frames = new Frame[bufferSize];
        for (int i = 0; i < bufferSize; ++i) {
            this.frames[i] = new Frame(new byte[DiskSpaceManager.PAGE_SIZE], i + 1);
        }
        this.firstFreeIndex = 0;
        this.diskSpaceManager = diskSpaceManager;
        this.pageToFrame = new HashMap<>();
        this.managerLock = new ReentrantLock();
        this.evictionPolicy = evictionPolicy;
        this.recoveryManager = recoveryManager;
    }

    @Override
    public void close() {
        this.managerLock.lock();
        try {
            for (Frame frame : this.frames) {
                frame.frameLock.lock();
                try {
                    if (frame.isPinned()) {
                        throw new IllegalStateException("closing buffer manager but frame still pinned");
                    }
                    if (!frame.isValid()) {
                        continue;
                    }
                    evictionPolicy.cleanup(frame);
                    frame.invalidate();
                } finally {
                    frame.frameLock.unlock();
                }
            }
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * 获取指定页面的缓冲帧数据。如果页面已加载到内存中，则重用现有的缓冲帧。锁定缓冲帧。
     * 不能在包外部使用。
     *
     * @param pageNum 页号
     * @return 包含指定页面的缓冲帧
     */
    Frame fetchPageFrame(long pageNum) {
        this.managerLock.lock();
        Frame newFrame;
        Frame evictedFrame;
        // 确定要加载数据到哪个帧，并更新管理器状态
        try {
            if (!this.diskSpaceManager.pageAllocated(pageNum)) {
                throw new PageException("page " + pageNum + " not allocated");
            }
            if (this.pageToFrame.containsKey(pageNum)) {
                newFrame = this.frames[this.pageToFrame.get(pageNum)];
                newFrame.pin();
                return newFrame;
            }
            // 优先使用空闲帧而不是驱逐
            if (this.firstFreeIndex < this.frames.length) {
                evictedFrame = this.frames[this.firstFreeIndex];
                evictedFrame.setUsed();
            } else {
                evictedFrame = (Frame) evictionPolicy.evict(frames);
                this.pageToFrame.remove(evictedFrame.pageNum, evictedFrame.index);
                evictionPolicy.cleanup(evictedFrame);
            }
            int frameIndex = evictedFrame.index;
            newFrame = this.frames[frameIndex] = new Frame(evictedFrame.contents, frameIndex, pageNum);
            evictionPolicy.init(newFrame);

            evictedFrame.frameLock.lock();
            newFrame.frameLock.lock();

            this.pageToFrame.put(pageNum, frameIndex);
        } finally {
            this.managerLock.unlock();
        }
        // 刷新被驱逐的帧
        try {
            evictedFrame.invalidate();
        } finally {
            evictedFrame.frameLock.unlock();
        }
        // 读取新页面到帧中
        try {
            newFrame.pageNum = pageNum;
            newFrame.pin();
            BufferManager.this.diskSpaceManager.readPage(pageNum, newFrame.contents);
            this.incrementIOs();
            return newFrame;
        } catch (PageException e) {
            newFrame.unpin();
            throw e;
        } finally {
            newFrame.frameLock.unlock();
        }
    }

    /**
     * 获取指定页面，带有一个已加载和锁定的缓冲帧。
     *
     * @param parentContext 被获取页面的**父级**的锁上下文
     * @param pageNum       页号
     * @return 指定页面
     */
    public Page fetchPage(LockContext parentContext, long pageNum) {
        return this.frameToPage(parentContext, pageNum, this.fetchPageFrame(pageNum));
    }

    /**
     * 获取新页面的缓冲帧。锁定缓冲帧。不能在包外部使用。
     *
     * @param partNum 新页面的分区号
     * @return 新页面的缓冲帧
     */
    Frame fetchNewPageFrame(int partNum) {
        long pageNum = this.diskSpaceManager.allocPage(partNum);
        this.managerLock.lock();
        try {
            return fetchPageFrame(pageNum);
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * 获取新页面，带有一个已加载和锁定的缓冲帧。
     *
     * @param parentContext 新页面的父锁上下文
     * @param partNum       分区号
     * @return 新页面
     */
    public Page fetchNewPage(LockContext parentContext, int partNum) {
        Frame newFrame = this.fetchNewPageFrame(partNum);
        return this.frameToPage(parentContext, newFrame.getPageNum(), newFrame);
    }

    /**
     * 释放页面 - 从缓存中驱逐页面，并告知磁盘空间管理器
     * 该页面不再需要。调用此方法前页面必须被锁定，
     * 调用后不能使用此页面（除了取消锁定）。
     *
     * @param page 要释放的页面
     */
    public void freePage(Page page) {
        this.managerLock.lock();
        try {
            TransactionContext transaction = TransactionContext.getTransaction();
            int frameIndex = this.pageToFrame.get(page.getPageNum());

            Frame frame = this.frames[frameIndex];
            if (transaction != null) page.flush();
            this.pageToFrame.remove(page.getPageNum(), frameIndex);
            evictionPolicy.cleanup(frame);
            frame.setFree();

            this.frames[frameIndex] = new Frame(frame);
            diskSpaceManager.freePage(page.getPageNum());
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * 释放分区 - 从缓存中驱逐所有相关页面，并告知磁盘空间管理器
     * 该分区不再需要。调用此方法前分区中的任何页面都不能被锁定，
     * 调用后不能使用这些页面。
     *
     * @param partNum 要释放的分区号
     */
    public void freePart(int partNum) {
        this.managerLock.lock();
        try {
            for (int i = 0; i < frames.length; ++i) {
                Frame frame = frames[i];
                if (DiskSpaceManager.getPartNum(frame.pageNum) == partNum) {
                    this.pageToFrame.remove(frame.getPageNum(), i);
                    evictionPolicy.cleanup(frame);
                    frame.flush();
                    frame.setFree();
                    frames[i] = new Frame(frame);
                }
            }

            diskSpaceManager.freePart(partNum);
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * 对页面的帧调用flush，并从帧中卸载页面。如果页面
     * 未加载，则此操作不执行任何操作。
     * @param pageNum 要驱逐的页面号
     */
    public void evict(long pageNum) {
        managerLock.lock();
        try {
            if (!pageToFrame.containsKey(pageNum)) {
                return;
            }
            evict(pageToFrame.get(pageNum));
        } finally {
            managerLock.unlock();
        }
    }

    /**
     * 驱逐页面
     * */
    private void evict(int i) {
        Frame frame = frames[i];
        frame.frameLock.lock();
        try {
            if (frame.isValid() && !frame.isPinned()) {
                this.pageToFrame.remove(frame.pageNum, frame.index);
                evictionPolicy.cleanup(frame);

                frames[i] = new Frame(frame.contents, this.firstFreeIndex);
                this.firstFreeIndex = i;

                frame.invalidate();
            }
        } finally {
            frame.frameLock.unlock();
        }
    }

    /**
     * 按顺序对每个帧调用驱逐。
     */
    public void evictAll() {
        for (int i = 0; i < frames.length; ++i) {
            evict(i);
        }
    }

    /**
     * 使用每个已加载页面的页号调用传入的方法。
     * @param process 消耗页号的方法。第一个参数是页号，
     *                第二个参数是布尔值，指示页面是否已修改
     *                （有未刷新的更改）。
     */
    public void iterPageNums(BiConsumer<Long, Boolean> process) {
        for (Frame frame : frames) {
            frame.frameLock.lock();
            try {
                if (frame.isValid()) {
                    process.accept(frame.pageNum, frame.dirty);
                }
            } finally {
                frame.frameLock.unlock();
            }
        }
    }

    /**
     * 获取自缓冲管理器启动以来的I/O次数，不包括磁盘空间管理中使用的任何内容，
     * 也不计算分配/释放。这除了作为相对测量外，实际上没有用处。
     * @return I/O次数
     */
    public long getNumIOs() {
        return numIOs;
    }

    public static boolean logIOs;
    private void incrementIOs() {
        if (logIOs) {
            System.out.println("IO incurred");
            StackTraceElement[] trace = Thread.currentThread().getStackTrace();
            for (int i = 0; i < trace.length; i++) {
                String s = trace[i].toString();
                if (s.startsWith("edu")) {
                    System.out.println(s);
                }
            }
        }
        ++numIOs;
    }

    /**
     * 将帧包装在页面对象中。
     * @param parentContext 页面的父锁上下文
     * @param pageNum 页号
     * @param frame 页面的帧
     * @return 页面对象
     */
    private Page frameToPage(LockContext parentContext, long pageNum, Frame frame) {
        return new Page(parentContext.childContext(pageNum), frame);
    }
}
