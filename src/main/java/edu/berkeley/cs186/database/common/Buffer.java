package edu.berkeley.cs186.database.common;

/**
 * 缓冲区用于存储字节序列，例如当我们需要将信息序列化为可存储在磁盘上的字节序列，
 * 并将序列反序列化回 Java 对象时。Put 方法将返回缓冲区本身，允许将 put 调用链接在一起。例如：
 *
 * ByteBuffer b = ByteBuffer.allocate(6);
 * // 缓冲区内容：空
 * b.putChar('c').putChar('s').putInt(186);
 * // 缓冲区内容：|0x63, 0x73, 0x00, 0x00, 0x00, 0xBA|
 *
 * 调用 get 将从缓冲区的开头（或指定索引）反序列化字节，并将缓冲区开头移动到下一个未读字节。
 * 重用上面的缓冲区：
 *
 * char char1 = b.getChar(); // char1 = 'c'
 * // 缓冲区内容：|0x73, 0x00, 0x00, 0x00, 0xBA|
 * char char2 = b.getChar(); // char2 = 's'
 * // 缓冲区内容：|0x00, 0x00, 0x00, 0xBA|
 * int num = b.getInt(); // num = 186
 * // 缓冲区内容：空
 *
 * 缓冲区无法知道原始数据类型是什么，因此需要以与序列化时相同的方式反序列化内容。
 * 例如，立即调用 b.getInt() 将尝试将前 4 个字节（0x63, 0x73, 0x00, 0x00）作为整数读取，
 * 尽管前两个字节是字符的一部分，而不是整数。
 *
 * 通常，您需要按照 put 操作发生的相同顺序调用 get 操作。
 */
public interface Buffer {
    Buffer get(byte[] dst, int offset, int length);
    byte get(int index);
    byte get();
    Buffer get(byte[] dst);
    char getChar();
    char getChar(int index);
    double getDouble();
    double getDouble(int index);
    float getFloat();
    float getFloat(int index);
    int getInt();
    int getInt(int index);
    long getLong();
    long getLong(int index);
    short getShort();
    short getShort(int index);
    Buffer put(byte[] src, int offset, int length);
    Buffer put(byte[] src);
    Buffer put(byte b);
    Buffer put(int index, byte b);
    Buffer putChar(char value);
    Buffer putChar(int index, char value);
    Buffer putDouble(double value);
    Buffer putDouble(int index, double value);
    Buffer putFloat(float value);
    Buffer putFloat(int index, float value);
    Buffer putInt(int value);
    Buffer putInt(int index, int value);
    Buffer putLong(long value);
    Buffer putLong(int index, long value);
    Buffer putShort(short value);
    Buffer putShort(int index, short value);
    Buffer slice();
    Buffer duplicate();
    int position();
    Buffer position(int pos);
}
