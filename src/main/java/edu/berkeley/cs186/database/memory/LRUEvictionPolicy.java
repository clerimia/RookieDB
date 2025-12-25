package edu.berkeley.cs186.database.memory;

/**
 * LRU（最近最少使用）淘汰策略的实现，该策略通过在帧之间创建按使用时间升序排列的双向链表来工作。
 */
public class LRUEvictionPolicy implements EvictionPolicy {
    private Tag listHead;
    private Tag listTail;

    // 帧之间的双向链表，按最近最少使用到最近最多使用的顺序排列
    private class Tag {
        Tag prev = null;
        Tag next = null;
        BufferFrame cur = null;

        @Override
        public String toString() {
            String sprev = (prev == null || prev.cur == null) ? "null" : prev.cur.toString();
            String snext = (next == null || next.cur == null) ? "null" : next.cur.toString();
            String scur = cur == null ? "null" : cur.toString();
            return scur + " (prev=" + sprev + ", next=" + snext + ")";
        }
    }

    public LRUEvictionPolicy() {
        this.listHead = new Tag();
        this.listTail = new Tag();
        this.listHead.next = this.listTail;
        this.listTail.prev = this.listHead;
    }

    /**
     * 调用以初始化新的缓冲区帧。
     * @param frame 要初始化的新帧
     */
    @Override
    public void init(BufferFrame frame) {
        Tag frameTag = new Tag();
        frameTag.next = listTail;
        frameTag.prev = listTail.prev;
        listTail.prev = frameTag;
        frameTag.prev.next = frameTag;
        frameTag.cur = frame;
        frame.tag = frameTag;
    }

    /**
     * 当帧被访问时调用。
     * @param frame 正在读取/写入的帧对象
     */
    @Override
    public void hit(BufferFrame frame) {
        Tag frameTag = (Tag) frame.tag;
        frameTag.prev.next = frameTag.next;
        frameTag.next.prev = frameTag.prev;
        frameTag.next = this.listTail;
        frameTag.prev = this.listTail.prev;
        this.listTail.prev.next = frameTag;
        this.listTail.prev = frameTag;
    }

    /**
     * 当需要淘汰帧时调用。
     * @param frames 所有帧的数组（每次调用长度相同）
     * @return 要被淘汰的帧的索引
     * @throws IllegalStateException 如果所有帧都被固定
     */
    @Override
    public BufferFrame evict(BufferFrame[] frames) {
        Tag frameTag = this.listHead.next;
        while (frameTag.cur != null && frameTag.cur.isPinned()) {
            frameTag = frameTag.next;
        }
        if (frameTag.cur == null) {
            throw new IllegalStateException("无法淘汰任何内容 - 所有帧都被固定");
        }
        return frameTag.cur;
    }

    /**
     * 当帧被移除时调用，要么是因为它从淘汰调用中返回，
     * 要么是因为其他约束条件（例如磁盘上的页面被删除）。
     * @param frame 正在移除的帧
     */
    @Override
    public void cleanup(BufferFrame frame) {
        Tag frameTag = (Tag) frame.tag;
        frameTag.prev.next = frameTag.next;
        frameTag.next.prev = frameTag.prev;
        frameTag.prev = frameTag.next = frameTag;
    }
}
