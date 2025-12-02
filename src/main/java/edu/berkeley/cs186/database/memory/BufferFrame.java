package edu.berkeley.cs186.database.memory;

/**
 * 缓冲区帧。
 */
abstract class BufferFrame {
    Object tag = null;
    private int pinCount = 0;

    /**
     * 固定缓冲区帧；固定时不能被驱逐。当缓冲区帧被固定时会发生"命中"。
     */
    void pin() {
        ++pinCount;
    }

    /**
     * 解除缓冲区帧的固定。
     */
    void unpin() {
        if (!isPinned()) {
            throw new IllegalStateException("无法解除未固定的帧");
        }
        --pinCount;
    }

    /**
     * @return 此帧是否被固定
     */
    boolean isPinned() {
        return pinCount > 0;
    }

    /**
     * @return 此帧是否有效
     */
    abstract boolean isValid();

    /**
     * @return 此帧的页号
     */
    abstract long getPageNum();

    /**
     * 将此缓冲区帧刷新到磁盘，但不卸载它。
     */
    abstract void flush();

    /**
     * 从缓冲区帧读取。
     * @param position 开始读取的缓冲区帧位置
     * @param num 要读取的字节数
     * @param buf 输出缓冲区
     */
    abstract void readBytes(short position, short num, byte[] buf);

    /**
     * 写入缓冲区帧，并将帧标记为脏。
     * @param position 开始写入的缓冲区帧位置
     * @param num 要写入的字节数
     * @param buf 输入缓冲区
     */
    abstract void writeBytes(short position, short num, byte[] buf);

    /**
     * 请求页面的有效帧对象（如果无效，则返回新的帧对象）。
     * 返回时帧被固定。
     */
    abstract BufferFrame requestValidFrame();

    /**
     * @return 帧用户可用的空间量
     */
    short getEffectivePageSize() {
        return BufferManager.EFFECTIVE_PAGE_SIZE;
    }

    /**
     * @param pageLSN 加载到此帧中的页面的新pageLSN
     */
    abstract void setPageLSN(long pageLSN);

    /**
     * @return 加载到此帧中的页面的pageLSN
     */
    abstract long getPageLSN();
}
