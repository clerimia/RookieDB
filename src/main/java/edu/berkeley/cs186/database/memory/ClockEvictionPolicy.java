package edu.berkeley.cs186.database.memory;

/**
 * 时钟淘汰策略的实现，该策略通过为每个帧添加一个引用位并运行算法来工作。
 */
public class ClockEvictionPolicy implements EvictionPolicy {
    private int arm;

    private static final Object ACTIVE = true;
    // 使用null而不是false，因为默认标记（在该类首次看到帧之前）是null。
    private static final Object INACTIVE = null;

    public ClockEvictionPolicy() {
        this.arm = 0;
    }

    /**
     * 调用以初始化新的缓冲帧。
     * @param frame 要初始化的新帧
     */
    @Override
    public void init(BufferFrame frame) {}

    /**
     * 当帧被访问时调用。
     * @param frame 正在被读取/写入的帧对象
     */
    @Override
    public void hit(BufferFrame frame) {
        frame.tag = ACTIVE;
    }

    /**
     * 当需要淘汰帧时调用。
     * @param frames 所有帧的数组（每次调用长度相同）
     * @return 要被淘汰的帧的索引
     * @throws IllegalStateException 如果所有帧都被固定
     */
    @Override
    public BufferFrame evict(BufferFrame[] frames) {
        int iters = 0;
        // 循环遍历帧，寻找标记位为0的帧
        // 使用iters确保不会无限循环 - 在遍历两轮后，每个帧的标记位都为0，
        // 因此如果仍然没有找到合适的页面进行淘汰，则说明所有帧都被固定了。
        while ((frames[this.arm].tag == ACTIVE || frames[this.arm].isPinned()) &&
                iters < 2 * frames.length) {
            frames[this.arm].tag = INACTIVE;
            this.arm = (this.arm + 1) % frames.length;
            ++iters;
        }
        if (iters == 2 * frames.length) {
            throw new IllegalStateException("无法淘汰 - 所有帧都被固定");
        }
        BufferFrame evicted = frames[this.arm];
        this.arm = (this.arm + 1) % frames.length;
        return evicted;
    }

    /**
     * 当帧被移除时调用，无论是因为它从evict调用中返回，
     * 还是由于其他约束条件（例如磁盘上的页面被删除）。
     * @param frame 正在被移除的帧
     */
    @Override
    public void cleanup(BufferFrame frame) {}
}
