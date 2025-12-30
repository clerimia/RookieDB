package edu.berkeley.cs186.database.recovery.records;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.recovery.LogRecord;
import edu.berkeley.cs186.database.recovery.LogType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class EndCheckpointLogRecord extends LogRecord {
    private Map<Long, Long> dirtyPageTable;
    private Map<Long, Pair<Transaction.Status, Long>> transactionTable;

    public EndCheckpointLogRecord(Map<Long, Long> dirtyPageTable,
                                  Map<Long, Pair<Transaction.Status, Long>> transactionTable) {
        super(LogType.END_CHECKPOINT);
        this.dirtyPageTable = new HashMap<>(dirtyPageTable);
        this.transactionTable = new HashMap<>(transactionTable);
    }

    @Override
    public Map<Long, Long> getDirtyPageTable() {
        return dirtyPageTable;
    }

    @Override
    public Map<Long, Pair<Transaction.Status, Long>> getTransactionTable() {
        return transactionTable;
    }

    @Override
    public byte[] toBytes() {
        int recordSize = getRecordSize(dirtyPageTable.size(), transactionTable.size());
        byte[] b = new byte[recordSize];
        Buffer buf = ByteBuffer.wrap(b)
                     .put((byte) getType().getValue())
                     .putShort((short) dirtyPageTable.size())
                     .putShort((short) transactionTable.size());
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            buf.putLong(entry.getKey()).putLong(entry.getValue());
        }
        for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : transactionTable.entrySet()) {
            buf.putLong(entry.getKey())
            .put((byte) entry.getValue().getFirst().ordinal())
            .putLong(entry.getValue().getSecond());
        }
        return b;
    }

    /**
     * @return 记录的字节大小
     */
    public static int getRecordSize(int numDPTRecords, int numTxnTableRecords) {
        // 1 字节用于记录类型，2 字节用于 DPT 大小，2 字节用于事务表大小
        // DPT: long -> long (16 字节)
        // 事务: long -> (byte, long) (17 字节)
        return 5 + 16 * numDPTRecords + 17 * numTxnTableRecords;
    }

    /**
     * @return 布尔值，表示日志记录的信息是否可以容纳在页面的一个记录中
     */
    public static boolean fitsInOneRecord(int numDPTRecords, int numTxnTableRecords) {
        int recordSize = getRecordSize(numDPTRecords, numTxnTableRecords);
        return recordSize <= DiskSpaceManager.PAGE_SIZE;
    }

    public static Optional<LogRecord> fromBytes(Buffer buf) {
        short dptSize = buf.getShort();
        short xactSize = buf.getShort();
        Map<Long, Long> dirtyPageTable = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> transactionTable = new HashMap<>();
        for (short i = 0; i < dptSize; ++i) {
            dirtyPageTable.put(buf.getLong(), buf.getLong());
        }
        for (short i = 0; i < xactSize; ++i) {
            long transNum = buf.getLong();
            Transaction.Status status = Transaction.Status.fromInt(buf.get());
            long lastLSN = buf.getLong();
            transactionTable.put(transNum, new Pair<>(status, lastLSN));
        }
        return Optional.of(new EndCheckpointLogRecord(dirtyPageTable, transactionTable));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        EndCheckpointLogRecord that = (EndCheckpointLogRecord) o;
        return dirtyPageTable.equals(that.dirtyPageTable) &&
               transactionTable.equals(that.transactionTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dirtyPageTable, transactionTable);
    }

    @Override
    public String toString() {
        return "EndCheckpointLogRecord{" +
               "dirtyPageTable=" + dirtyPageTable +
               ", transactionTable=" + transactionTable +
               ", LSN=" + LSN +
               '}';
    }
}
