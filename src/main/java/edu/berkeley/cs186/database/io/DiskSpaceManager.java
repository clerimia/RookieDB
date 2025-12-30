package edu.berkeley.cs186.database.io;

public interface DiskSpaceManager extends AutoCloseable {
    short PAGE_SIZE = 4096; // 页面大小（字节）
    long INVALID_PAGE_NUM = -1L; // 始终无效的页号

    @Override
    void close();

    /**
     * 分配一个新分区。
     *
     * @return 新分区的分区号
     */
    int allocPart();

    /**
     * 分配一个具有特定分区号的新分区。
     *
     * @param partNum 新分区的分区号
     * @return 新分区的分区号
     */
    int allocPart(int partNum);

    /**
     * 释放一个分区。
     *
     * @param partNum 要释放的分区号
     */
    void freePart(int partNum);

    /**
     * 分配一个新页面。
     * @param partNum 要在其中分配新页面的分区
     * @return 新页面的虚拟页号
     */
    long allocPage(int partNum);

    /**
     * 分配一个具有特定页号的新页面。
     * @param pageNum 新页面的页号
     * @return 新页面的虚拟页号
     */
    long allocPage(long pageNum);

    /**
     * 释放一个页面。调用后该页面不能再使用。
     * @param page 要释放的页面的虚拟页号
     */
    void freePage(long page);

    /**
     * 读取一个页面。
     *
     * @param page 要读取的页面号
     * @param buf 字节数组缓冲区，其内容将被页面数据填充
     */
    void readPage(long page, byte[] buf);

    /**
     * 写入一个页面。
     *
     * @param page 要写入的页面号
     * @param buf 包含新页面数据的字节数组缓冲区
     */
    void writePage(long page, byte[] buf);

    /**
     * 检查页面是否已分配
     *
     * @param page 要检查的页面号
     * @return 如果页面已分配则返回true，否则返回false
     */
    boolean pageAllocated(long page);

    /**
     * 从虚拟页号获取分区号
     * @param page 虚拟页号
     * @return 分区号
     */
    static int getPartNum(long page) {
        return (int) (page / 10000000000L);
    }

    /**
     * 从虚拟页号获取数据页号
     * @param page 虚拟页号
     * @return 数据页号
     */
    static int getPageNum(long page) {
        return (int) (page % 10000000000L);
    }

    /**
     * 根据分区号和数据页号获取虚拟页号
     * @param partNum 分区号
     * @param pageNum 数据页号
     * @return 虚拟页号
     */
    static long getVirtualPageNum(int partNum, int pageNum) {
        return partNum * 10000000000L + pageNum;
    }

}
