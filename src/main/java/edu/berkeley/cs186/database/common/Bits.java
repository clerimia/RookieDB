package edu.berkeley.cs186.database.common;

/**
 * 用于获取、设置和计算字节或字节数组中位的工具类
 */
public class Bits {
    public enum Bit { ZERO, ONE }

    /**
     * 获取字节的第i位，其中第0位是最高有效位，第7位是最低有效位。一些示例：
     *
     *   - getBit(0b10000000, 7) == ZERO
     *   - getBit(0b10000000, 0) == ONE
     *   - getBit(0b01000000, 1) == ONE
     *   - getBit(0b00100000, 1) == ZERO
     */
    static Bit getBit(byte b, int i) {
        if (i < 0 || i >= 8) {
            throw new IllegalArgumentException(String.format("index %d out of bounds", i));
        }
        return ((b >> (7 - i)) & 1) == 0 ? Bit.ZERO : Bit.ONE;
    }

    /**
     * 获取字节数组的第i位，其中第0位是第一个字节的最高有效位。一些示例：
     *
     *   - getBit(new byte[]{0b10000000, 0b00000000}, 0) == ONE
     *   - getBit(new byte[]{0b01000000, 0b00000000}, 1) == ONE
     *   - getBit(new byte[]{0b00000000, 0b00000001}, 15) == ONE
     */
    public static Bit getBit(byte[] bytes, int i) {
        if (bytes.length == 0 || i < 0 || i >= bytes.length * 8) {
            String err = String.format("bytes.length = %d; i = %d.", bytes.length, i);
            throw new IllegalArgumentException(err);
        }
        return getBit(bytes[i / 8], i % 8);
    }

    /**
     * 设置字节的第i位，其中第0位是最高有效位，第7位是最低有效位。一些示例：
     *
     *   - setBit(0b00000000, 0, ONE) == 0b10000000
     *   - setBit(0b00000000, 1, ONE) == 0b01000000
     *   - setBit(0b00000000, 2, ONE) == 0b00100000
     */
    static byte setBit(byte b, int i, Bit bit) {
        if (i < 0 || i >= 8) {
            throw new IllegalArgumentException(String.format("index %d out of bounds", i));
        }
        byte mask = (byte) (1 << (7 - i));
        switch (bit) {
        case ZERO: { return (byte) (b & ~mask); }
        case ONE: { return (byte) (b | mask); }
        default: { throw new IllegalArgumentException("Unreachable code."); }
        }
    }

    /**
     * 设置字节数组的第i位，其中第0位是第一个字节(arr[0])的最高有效位。一个示例：
     *
     *   byte[] buf = new bytes[2]; // [0b00000000, 0b00000000]
     *   setBit(buf, 0, ONE); // [0b10000000, 0b00000000]
     *   setBit(buf, 1, ONE); // [0b11000000, 0b00000000]
     *   setBit(buf, 2, ONE); // [0b11100000, 0b00000000]
     *   setBit(buf, 15, ONE); // [0b11100000, 0b00000001]
     */
    public static void setBit(byte[] bytes, int i, Bit bit) {
        bytes[i / 8] = setBit(bytes[i / 8], i % 8, bit);
    }

    /**
     * 计算设置位的数量。例如：
     *
     *   - countBits(0b00001010) == 2
     *   - countBits(0b11111101) == 7
     */
    public static int countBits(byte b) {
        return Integer.bitCount(b);
    }

    /**
     * 计算设置位的数量。
     */
    public static int countBits(byte[] bytes) {
        int count = 0;
        for (byte b : bytes) {
            count += countBits(b);
        }
        return count;
    }
}
