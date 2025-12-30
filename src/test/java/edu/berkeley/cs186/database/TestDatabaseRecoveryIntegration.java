package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * 这些测试*不*进行评分，也不属于你的项目5提交内容，但如果你完成了项目5，这些测试应该都能通过。
 */
public class TestDatabaseRecoveryIntegration {
    private static final String TestDir = "testDatabaseRecovery";
    private Database db;
    private LockManager lockManager;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 10秒每个方法测试的最大时间。
    public static long timeout = (long) (10000 * TimeoutScaling.factor);

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis(timeout));

    private void reloadDatabase(boolean closeOld) {
        if (closeOld && this.db != null) {
            this.db.close();
        }
        if (TransactionContext.getTransaction() != null) {
            TransactionContext.unsetTransaction();
        }
        this.lockManager = new LockManager();
        this.db = new Database(this.filename, 128, this.lockManager, new ClockEvictionPolicy(), true);
        this.db.setWorkMem(32); // B=32

        if (closeOld) {
            try {
                this.db.loadDemo();
            } catch (IOException e) {
                throw new DatabaseException("Failed to load demo tables.\n" + e.getMessage());
            }
        }
        // 强制初始化完成后再继续
        this.db.waitAllTransactions();
    }

    private void reloadDatabase() {
        reloadDatabase(true);
    }

    @ClassRule
    public static  TemporaryFolder checkFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.reloadDatabase();
        this.db.waitAllTransactions();
    }

    @Test
    public void testRollbackDropTable() {
        /**
         * 测试在删除表时回滚是否正常工作。对`Students`表进行全表扫描并存储记录。
         * 然后，删除`Students`表。删除后，对`Students`表的查询应该失败。
         * 之后，回滚事务。
         *
         * 在新事务中，对`Students`进行新的全表扫描以验证所有记录是否已恢复。
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // 对`Students`进行全表扫描
        try (Transaction t= db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.dropTable("Students");
            try {
                t.query("Students").execute();
                fail("Query should have failed, Students was dropped!");
            } catch (DatabaseException e) {
                // 确保失败原因正确
                assertTrue(e.getMessage().contains("does not exist!"));
            }
            t.rollback();
        }

        try (Transaction t= db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRollbackDropAllTables() {
        /**
         * 与上面的测试相同，但检查删除所有表的行为。
         */
        String[] tableNames = {"Students", "Enrollments", "Courses"};
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // 对每个表进行全表扫描
        try (Transaction t= db.beginTransaction()) {
            for (int i = 0; i < tableNames.length; ++i) {
                Iterator<Record> records = t.query(tableNames[i]).execute();
                while (records.hasNext()) oldRecords.add(records.next());
            }
            // 删除所有表
            t.dropAllTables();
            for (int i = 0; i < tableNames.length; ++i) {
                try {
                    t.query(tableNames[i]).execute();
                    fail("Query should have failed, all tables were dropped!");
                } catch (DatabaseException e) {
                    // 确保失败原因正确
                    assertTrue(e.getMessage().contains("does not exist!"));
                }
            }
            t.rollback();
        }

        try (Transaction t= db.beginTransaction()) {
            for (int i = 0; i < tableNames.length; ++i) {
                Iterator<Record> records = t.query(tableNames[i]).execute();
                while (records.hasNext()) newRecords.add(records.next());
            }
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRollbackDropIndex() {
        /**
         * 在`Students.sid`上创建索引。由于索引可用，
         * 对`sid`的等值查询应在查询计划中使用索引扫描来访问表。
         * 然后删除索引，查询计划不应再有索引扫描的访问方式。
         * 之后，回滚DROP INDEX操作，查询应再次依赖索引扫描。
         */
        try (Transaction t = db.beginTransaction()) {
            t.createIndex("Students", "sid", false);
        }

        try (Transaction t = db.beginTransaction()) {
            QueryPlan p1 = t.query("Students");
            p1.select("sid", PredicateOperator.EQUALS, 186);
            p1.execute();
            assertTrue(p1.getFinalOperator().toString().contains("Index Scan"));
            t.dropIndex("Students", "sid");
            QueryPlan p2 = t.query("Students");
            p2.select("sid", PredicateOperator.EQUALS, 186);
            p2.execute();
            assertFalse(p2.getFinalOperator().toString().contains("Index Scan"));
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            QueryPlan p3 = t.query("Students");
            p3.select("sid", PredicateOperator.EQUALS, 186);
            p3.execute();
            assertTrue(p3.getFinalOperator().toString().contains("Index Scan"));
        }
        this.db.close();
    }

    @Test
    public void testRollbackUpdate() {
        /**
         * 对`Students`表进行全表扫描并将结果保存以供后续比较。
         * 然后将每个学生的`gpa`字段设置为0.0，并再次进行全表扫描以验证更改是否已生效。
         * 最后，回滚更改并比较回滚事务之前和之后`Students`的状态。
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            // 拉平曲线
            t.update("Students", "gpa", (DataBox d) -> DataBox.fromObject(0.0));
            Schema s = t.getSchema("Students");
            records = t.query("Students").execute();
            while (records.hasNext()) {
                assertEquals(DataBox.fromObject(0.0), records.next().getValue(s.findField("gpa")));
            }
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
    }

    @Test
    public void testRollbackDeleteAll() {
        /**
         * 与上面相同，但不是更新记录而是删除它们。
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.delete("Students", "gpa", PredicateOperator.GREATER_THAN_EQUALS, DataBox.fromObject(0.0));
            records = t.query("Students").execute();
            assertFalse(records.hasNext());
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
    }

    @Test
    public void testRollbackDeletePartial() {
        /**
         * 与上面相同，但只删除特定记录。
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.delete("Students", "gpa", PredicateOperator.GREATER_THAN_EQUALS, DataBox.fromObject(2.0));
            Schema s = t.getSchema("Students");
            records = t.query("Students").execute();
            while (records.hasNext()) {
                assertTrue(records.next().getValue(s.findField("gpa")).getFloat() < 2.0);
            }
            t.rollback();
        }

        try (Transaction t = db.beginTransaction()) {
            Iterator<Record> records = t.query("Students").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
    }

    @Test
    public void testSavepointDropTable() {
        /**
         * 测试删除表时保存点的功能。
         * 首先，对`Enrollments`进行全表扫描并将记录存储以供后续比较。
         *
         * 接下来，删除Students表，并创建保存点。
         * 删除`Enrollments`表，然后立即回滚到保存点。
         * 由于我们已回滚到`Enrollments`被删除之前，表应该被恢复，
         * 当我们再次进行全表扫描时，所有原始记录应该都存在。
         *
         * 最后，在新事务中，我们检查`Students`表是否不再存在，
         * 因为我们没有回滚删除`Students`的操作，
         * 并再次检查`Enrollments`中的所有原始记录是否存在。
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // 对`Enrollments`进行全表扫描
        try (Transaction t= db.beginTransaction()) {
            Iterator<Record> records = t.query("Enrollments").execute();
            while (records.hasNext()) oldRecords.add(records.next());

            t.dropTable("Students");
            t.savepoint("beforeDroppingEnrollments");
            t.dropTable("Enrollments");
            try {
                t.query("Enrollments").execute();
                fail("Query should have failed, Enrollments was dropped!");
            } catch (DatabaseException e) {
                // 确保失败原因正确
                assertTrue(e.getMessage().contains("does not exist!"));
            }
            t.rollbackToSavepoint("beforeDroppingEnrollments");
            List<Record> afterRollbackRecords = new ArrayList<>();
            records = t.query("Enrollments").execute();
            while (records.hasNext()) afterRollbackRecords.add(records.next());
            assertEquals(oldRecords, afterRollbackRecords);
        }

        try (Transaction t= db.beginTransaction()) {
            // `DROP TABLE Students`未被回滚，尝试查询时仍应失败。
            try {
                t.query("Students").execute();
                fail("Query should have failed, Students was dropped!");
            } catch (DatabaseException e) {
                // 确保失败原因正确
                assertTrue(e.getMessage().contains("does not exist!"));
            }

            Iterator<Record> records = t.query("Enrollments").execute();
            while (records.hasNext()) newRecords.add(records.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRebootDropTable() {
        /**
         * 模拟数据库在事务中途崩溃。在这种情况下，T1是一个删除`Students`表的事务，
         * 但在数据库重启之前没有提交。T2是在数据库恢复后创建的新事务，
         * 因此它应该能够访问`Students`表，就像它从未被删除过一样。
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // 对`Students`进行全表扫描
        Transaction t1 = db.beginTransaction();
        Iterator<Record> records = t1.query("Students").execute();
        while (records.hasNext()) oldRecords.add(records.next());

        t1.dropTable("Students");
        try {
            t1.query("Students").execute();
            fail("Query should have failed, Students was dropped!");
        } catch (DatabaseException e) {
            // 确保失败原因正确
            assertTrue(e.getMessage().contains("does not exist!"));
        }
        // 注意：T1从未提交！
        Database old = this.db;
        reloadDatabase(false);
        try (Transaction t2 = db.beginTransaction()) {
            Iterator<Record> records2 = t2.query("Students").execute();
            while (records2.hasNext()) newRecords.add(records2.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRebootPartialDelete() {
        /**
         * 与上面相同，但T1尝试对`Students`中的记录执行部分删除。
         */
        List<Record> oldRecords = new ArrayList<>();
        List<Record> newRecords = new ArrayList<>();

        // 对`Students`进行全表扫描
        Transaction t1 = db.beginTransaction();
        Iterator<Record> records = t1.query("Students").execute();
        while (records.hasNext()) oldRecords.add(records.next());

        t1.delete("Students", "gpa", PredicateOperator.GREATER_THAN_EQUALS, DataBox.fromObject(1.86));
        db.getBufferManager().evictAll();

        // 注意：更改已刷新，但T1从未提交！
        Database old = this.db;
        reloadDatabase(false);
        try (Transaction t2 = db.beginTransaction()) {
            Iterator<Record> records2 = t2.query("Students").execute();
            while (records2.hasNext()) newRecords.add(records2.next());
        }
        assertEquals(oldRecords, newRecords);
        this.db.close();
    }

    @Test
    public void testRebootCreateTable() {
        // 创建表，提交，然后重启
        try(Transaction t1 = db.beginTransaction()) {
            for (int i = 0; i < 3; i++) {
                t1.createTable(new Schema().add("int", Type.intType()), "ints" + i);
                for (int j = 0; j < 1024 * 5; j++) {
                    t1.insert("ints"+i, j);
                }
            }
        }
        Database old = this.db;
        reloadDatabase(false);
        try(Transaction t2 = db.beginTransaction()) {
            for (int i = 0; i < 3; i++) {
                Iterator<Record> records = t2.query("ints" + i).execute();
                assertTrue(records.next().getValue(0).getInt() == 0);
            }
        }
        this.db.close();
    }

    @Test
    public void testRebootCreateAndDropTable() {
        // 创建表，提交，然后重启
        try(Transaction t1 = db.beginTransaction()) {
            for (int i = 0; i < 3; i++) {
                t1.createTable(new Schema().add("int", Type.intType()), "ints" + i);
                for (int j = 0; j < 1024 * 5; j++) {
                    t1.insert("ints"+i, j);
                }
            }
            t1.dropTable("ints0");
        }
        Database old = this.db;
        reloadDatabase(false);
        try(Transaction t2 = db.beginTransaction()) {
            for (int i = 1; i < 3; i++) {
                Iterator<Record> records = t2.query("ints" + i).execute();
                assertTrue(records.next().getValue(0).getInt() == 0);
            }
        }
        this.db.close();
    }
}
