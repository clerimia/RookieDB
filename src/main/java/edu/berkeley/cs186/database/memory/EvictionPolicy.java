package edu.berkeley.cs186.database.memory;

/**
 * 缓冲区管理器的淘汰策略接口。
 */
public interface EvictionPolicy {
    /**
     * 初始化一个新的缓冲区帧。
     * @param frame 需要初始化的新帧
     */
    void init(BufferFrame frame);

    /**
     * 当帧被访问时调用。
     * @param frame 正在被读取/写入的帧对象
     */
    void hit(BufferFrame frame);

    /**
     * 当需要淘汰一个帧时调用。
     * @param frames 所有帧的数组（每次调用长度相同）
     * @return 需要被淘汰的帧的索引
     * @throws IllegalStateException 如果所有帧都被固定
     */
    BufferFrame evict(BufferFrame[] frames);

    /**
     * 当帧被移除时调用，可能是由于它从evict调用中返回，
     * 或者由于其他约束条件（例如磁盘上的页面被删除）。
     * @param frame 被移除的帧
     */
    void cleanup(BufferFrame frame);
}
