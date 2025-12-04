package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.databox.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 表的模式包括其每个字段的名称和类型。例如，以下模式：
 *
 *   Schema s = new Schema()
 *      .add("x", Type.intType())
 *      .add("y", Type.floatType());
 *
 * 表示一个表，该表具有名为"x"的整数字段和名为"y"的浮点字段。
 */
public class Schema {
    private List<String> fieldNames;
    private List<Type> fieldTypes;
    private short sizeInBytes;

    /**
     * 构造一个空的Schema。
     */
    public Schema() {
        this.fieldNames = new ArrayList<>();
        this.fieldTypes = new ArrayList<>();
        this.sizeInBytes = 0;
    }

    /**
     * 向模式中添加一个新字段。返回模式以便可以链接调用（见上面的例子）。
     * @param fieldName 新字段的名称
     * @param fieldType 新字段的类型
     * @return 添加字段的模式
     */
    public Schema add(String fieldName, Type fieldType) {
        this.fieldNames.add(fieldName);
        this.fieldTypes.add(fieldType);
        this.sizeInBytes += fieldType.getSizeInBytes();
        return this;
    }

    /**
     * @return 此模式中字段的名称，按顺序排列
     */
    public List<String> getFieldNames() {
        return fieldNames;
    }

    /**
     * @return 此模式中字段的类型，按顺序排列
     */
    public List<Type> getFieldTypes() {
        return fieldTypes;
    }

    /**
     * @param i
     * @return 索引`i`处字段的名称
     */
    public String getFieldName(int i) { return fieldNames.get(i); }

    /**
     * @param i
     * @return 索引`i`处字段的类型
     */
    public Type getFieldType(int i) { return fieldTypes.get(i); }

    /**
     * @return 此模式中字段的数量
     */
    public int size() { return this.fieldNames.size(); }

    /**
     * @return 序列化后此模式的字节大小
     */
    public short getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * @param fromSchema
     * @param specified
     * @return 如果两个名称可以被认为是相等的则返回true，否则返回false。
     * 当两个字段名称忽略大小写相同时，或者`specified`未限定且与`fromSchema`的
     * 未限定部分匹配时，两个字段名称相等。例如"table1.someCol"和"someCol"会被认为相等。
     */
    private static boolean fieldNamesEqual(String fromSchema, String specified) {
        fromSchema = fromSchema.toLowerCase();
        specified = specified.toLowerCase();
        if (fromSchema.equals(specified)) {
            return true;
        }
        if (!specified.contains(".")) {
            // specified未限定，从fromSchema中移除限定
            String schemaColName = fromSchema;
            if (fromSchema.contains(".")) {
                String[] splits = fromSchema.split("\\.");
                schemaColName = splits[1];
            }

            return schemaColName.equals(specified);
        }
        return false;
    }

    /**
     * @param fieldName
     * @throws RuntimeException 如果未找到字段，或字段名称不明确
     * @return 查找对应fieldName的字段索引
     */
    public int findField(String fieldName) {
        int index = -1;
        for (int i = 0; i < this.size(); i++) {
            String fromSchema = this.fieldNames.get(i);
            if (fieldNamesEqual(fromSchema, fieldName)) {
                if (index != -1) {
                    throw new RuntimeException("列 " + fieldName + " 在 " + toString() + " 中未消除歧义地指定两次");
                } else index = i;
            }
        }
        if (index == -1) throw new RuntimeException("在 " + toString() + " 中未找到列 " + fieldName);
        return index;
    }

    /**
     * @param fieldName
     * @return 提供字段在此模式中的名称，匹配大小写和限定
     */
    public String matchFieldName(String fieldName) {
        return this.fieldNames.get(this.findField(fieldName));
    }

    /**
     * @param other
     * @return 连接两个模式，返回一个新模式，其中包含此模式的字段紧接着`other`的字段
     */
    public Schema concat(Schema other) {
        Schema copy = new Schema();
        copy.fieldTypes = new ArrayList<>(fieldTypes);
        copy.fieldNames = new ArrayList<>(fieldNames);
        copy.sizeInBytes = sizeInBytes;
        for(int i = 0; i < other.size(); i++)
            copy.add(other.fieldNames.get(i), other.fieldTypes.get(i));
        return copy;
    }

    /**
     * 验证记录是否与给定模式匹配。执行以下隐式转换：
     * - 错误大小的字符串将转换为模式期望的大小
     * - 如果期望浮点数，整数将转换为浮点数
     * @param record
     * @throws DatabaseException 如果记录的字段与模式中相应字段的类型不匹配，
     * 并且无法隐式转换为正确字段
     * @return 一个新记录，其字段已转换以匹配模式
     */
    public Record verify(Record record) {
        List<DataBox> values = record.getValues();
        if (values.size() != fieldNames.size()) {
            String err = String.format("期望 %d 个值，但得到了 %d 个。",
                                       fieldNames.size(), values.size());
            throw new DatabaseException(err);
        }

        for (int i = 0; i < values.size(); ++i) {
            Type actual = values.get(i).type();
            Type expected = fieldTypes.get(i);
            if (!actual.equals(expected)) {
                if(actual.getTypeId() == TypeId.STRING && expected.getTypeId() == TypeId.STRING) {
                    // 隐式转换
                    DataBox wrongSize = values.get(i);
                    values.set(i, new StringDataBox(wrongSize.getString(), expected.getSizeInBytes()));
                    continue;
                }
                if(actual.getTypeId() == TypeId.INT && expected.getTypeId() == TypeId.FLOAT) {
                    // 隐式转换
                    DataBox intBox = values.get(i);
                    values.set(i, new FloatDataBox((float) intBox.getInt()));
                    continue;
                }
                String err = String.format(
                                 "期望字段 %d 的类型为 %s，但得到了类型为 %s 的值。",
                                 i, expected, actual);
                throw new DatabaseException(err);
            }
        }

        return new Record(values);
    }

    /**
     * @return 包含模式序列化副本的字节数组
     */
    public byte[] toBytes() {
        // 模式序列化如下。我们首先写入字段数量（4字节）。
        // 然后，对于每个字段，我们写入
        //
        //   1. 字段名称的长度（4字节），
        //   2. 字段名称，
        //   3. 字段类型。

        // 首先，我们计算序列化模式所需的字节数。
        int size = Integer.BYTES; // 模式的长度。
        for (int i = 0; i < fieldNames.size(); ++i) {
            size += Integer.BYTES; // 字段名称的长度。
            size += fieldNames.get(i).length(); // 字段名称。
            size += fieldTypes.get(i).toBytes().length; // 类型。
        }

        // 然后我们进行序列化。
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); ++i) {
            buf.putInt(fieldNames.get(i).length());
            buf.put(fieldNames.get(i).getBytes(Charset.forName("UTF-8")));
            buf.put(fieldTypes.get(i).toBytes());
        }
        return buf.array();
    }

    /**
     * 从缓冲区反序列化字节以创建Schema对象。此函数退出后，
     * 对缓冲区的下一次get()调用将是不属于模式的第一个字节。
     *
     * @param buf 从中提取字节的缓冲区。
     * @return 通过反序列化给定缓冲区中的字节创建的Schema对象
     */
    public static Schema fromBytes(Buffer buf) {
        Schema s = new Schema();
        int size = buf.getInt();
        for (int i = 0; i < size; i++) {
            int fieldSize = buf.getInt();
            byte[] bytes = new byte[fieldSize];
            buf.get(bytes);
            s.add(new String(bytes, Charset.forName("UTF-8")), Type.fromBytes(buf));
        }
        return s;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < fieldNames.size(); ++i) {
            sb.append(String.format("%s: %s", fieldNames.get(i), fieldTypes.get(i)));
            if (i != fieldNames.size()) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Schema)) return false;
        Schema s = (Schema) o;
        return fieldNames.equals(s.fieldNames) && fieldTypes.equals(s.fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames, fieldTypes);
    }
}
