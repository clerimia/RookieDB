package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.common.AbstractBuffer;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.PageException;

/**
 * 表示加载到内存中的页面（与所在的缓冲区帧相对）。包装
 * 缓冲区管理器帧，并在必要时请求将页面加载到内存中。
 */
public class Page {
    // 此页面的锁上下文
    private LockContext lockContext;

    // 此页面数据的缓冲区管理器帧（可能已失效）
    private BufferFrame frame;

    /**
     * 使用给定的缓冲区帧创建页面句柄
     *
     * @param lockContext 锁上下文
     * @param frame 此页面的缓冲区管理器帧
     */
    Page(LockContext lockContext, BufferFrame frame) {
        this.lockContext = lockContext;
        this.frame = frame;
    }

    /**
     * 根据另一个页面句柄创建页面句柄
     *
     * @param page 要复制的页面句柄
     */
    protected Page(Page page) {
        this.lockContext = page.lockContext;
        this.frame = page.frame;
    }

    /**
     * 禁用此页面句柄上的锁定。
     */
    public void disableLocking() {
        this.lockContext = new DummyLockContext("_dummyPage");
    }

    /**
     * 获取一个Buffer对象以便更方便地访问页面。
     *
     * @return 此页面上的Buffer对象
     */
    public Buffer getBuffer() {
        return new PageBuffer();
    }

    /**
     * 从偏移位置读取num个字节到buf中。
     *
     * @param position 页面中要读取的偏移量
     * @param num 要读取的字节数
     * @param buf 要放入字节的缓冲区
     */
    private void readBytes(int position, int num, byte[] buf) {
        if (position < 0 || num < 0) {
            throw new PageException("position或num不能为负数");
        }
        if (frame.getEffectivePageSize() < position + num) {
            throw new PageException("readBytes超出边界");
        }
        if (buf.length < num) {
            throw new PageException("要读取的字节数比缓冲区长");
        }

        this.frame.readBytes((short) position, (short) num, buf);
    }

    /**
     * 读取文件中的所有字节。
     *
     * @return 包含文件中所有字节的新字节数组
     */
    private byte[] readBytes() {
        byte[] data = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        getBuffer().get(data);
        return data;
    }

    /**
     * 在偏移位置从buf写入num个字节。
     *
     * @param position 文件中要写入的偏移量
     * @param num 要写入的字节数
     * @param buf 写入的源
     */
    private void writeBytes(int position, int num, byte[] buf) {
        if (buf.length < num) {
            throw new PageException("要写入的字节数比缓冲区长");
        }

        if (position < 0 || num < 0) {
            throw new PageException("position或num不能为负数");
        }

        if (frame.getEffectivePageSize() < num + position) {
            throw new PageException("writeBytes将超出边界");
        }

        this.frame.writeBytes((short) position, (short) num, buf);
    }

    /**
     * 写入文件中的所有字节。
     */
    private void writeBytes(byte[] data) {
        getBuffer().put(data);
    }

    /**
     * 完全擦除（清零）页面。
     */
    public void wipe() {
        byte[] zeros = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        writeBytes(zeros);
    }

    /**
     * 强制将页面刷新到磁盘。
     */
    public void flush() {
        this.frame.flush();
    }

    /**
     * 将页面加载到帧中（如有必要）并固定它。
     */
    public void pin() {
        this.frame = this.frame.requestValidFrame();
    }

    /**
     * 取消固定包含此页面的帧。不会立即刷新。
     */
    public void unpin() {
        this.frame.unpin();
    }

    /**
     * @return 此页面的虚拟页号
     */
    public long getPageNum() {
        return this.frame.getPageNum();
    }

    /**
     * @param pageLSN 此页面的新pageLSN - 应仅由恢复使用
     */
    public void setPageLSN(long pageLSN) {
        this.frame.setPageLSN(pageLSN);
    }

    /**
     * @return 此页面的pageLSN
     */
    public long getPageLSN() {
        return this.frame.getPageLSN();
    }

    @Override
    public String toString() {
        return "Page " + this.frame.getPageNum();
    }

    @Override
    public boolean equals(Object b) {
        if (!(b instanceof Page)) {
            return false;
        }
        return ((Page) b).getPageNum() == getPageNum();
    }

    /**
     * 页面数据的Buffer实现。所有读/写操作最终都包装在
     * Page#readBytes和Page#writeBytes周围，这些操作将工作委托给缓冲区管理器。
     */
    private class PageBuffer extends AbstractBuffer {
        private int offset;

        private PageBuffer() {
            this(0, 0);
        }

        private PageBuffer(int offset, int position) {
            super(position);
            this.offset = offset;
        }

        /**
         * 通过Page对象的所有读取操作都必须通过此方法运行。
         *
         * @param dst 目标字节缓冲区
         * @param offset 页面中开始读取的偏移量
         * @param length 要读取的字节数
         * @return this
         */
        @Override
        public Buffer get(byte[] dst, int offset, int length) {
            // TODO(proj4_part2): 更新以下行
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
            Page.this.readBytes(this.offset + offset, length, dst);
            return this;
        }

        /**
         * 通过Page对象的所有写入操作都必须通过此方法运行。
         *
         * @param src 源字节缓冲区（复制到页面）
         * @param offset 页面中开始写入的偏移量
         * @param length 要写入的字节数
         * @return this
         */
        @Override
        public Buffer put(byte[] src, int offset, int length) {
            // TODO(proj4_part2): 更新以下行
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
            Page.this.writeBytes(this.offset + offset, length, src);
            return this;
        }

        /**
         * 创建一个从当前偏移量开始的新PageBuffer。
         * @return 从当前偏移量开始的新PageBuffer
         */
        @Override
        public Buffer slice() {
            return new PageBuffer(offset + position(), 0);
        }

        /**
         * 创建重复的PageBuffer对象
         * @return 功能上与此对象相同的PageBuffer
         */
        @Override
        public Buffer duplicate() {
            return new PageBuffer(offset, position());
        }
    }
}
