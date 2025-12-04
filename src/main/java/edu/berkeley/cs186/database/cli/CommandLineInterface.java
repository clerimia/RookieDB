package edu.berkeley.cs186.database.cli;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.cli.parser.ASTSQLStatementList;
import edu.berkeley.cs186.database.cli.parser.ParseException;
import edu.berkeley.cs186.database.cli.parser.RookieParser;
import edu.berkeley.cs186.database.cli.parser.TokenMgrError;
import edu.berkeley.cs186.database.cli.visitor.StatementListVisitor;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CommandLineInterface {
    private static final String MASCOT = "\n\\|/  ___------___\n \\__|--%s______%s--|\n    |  %-9s |\n     ---______---\n";
    private static final int[] VERSION = { 1, 8, 6 }; // {主版本号, 次版本号, 构建号}
    private static final String LABEL = "fa24";

    private InputStream in;
    private PrintStream out; // 使用替代System.out以便在网络间工作
    private Database db;
    private Random generator;

    public static void main(String args[]) throws IOException {
        // 项目0到3的基础数据库
        Database db = new Database("demo", 25);
        
        // 完成项目4（锁定）后使用以下代码
        // Database db = new Database("demo", 25, new LockManager());
        
        // 完成项目5（恢复）后使用以下代码
        // Database db = new Database("demo", 25, new LockManager(), new ClockEvictionPolicy(), true);

        db.loadDemo();

        CommandLineInterface cli = new CommandLineInterface(db); // 初始化一个连接
        cli.run();
        db.close();
    }

    public CommandLineInterface(Database db) {
        // 默认情况下，只使用标准输入和输出
        this(db, System.in, System.out);
    }

    public CommandLineInterface(Database db, InputStream in, PrintStream out) {
        this.db = db;
        this.in = in;
        this.out = out;
        this.generator = new Random(); // 随机数生成器
    }

    public void run() {
        // 欢迎信息
        this.out.printf(MASCOT, "o", "o", institution[this.generator.nextInt(institution.length)]);
        this.out.printf("\n欢迎使用RookieDB (v%d.%d.%d-%s)\n", VERSION[0], VERSION[1], VERSION[2], LABEL);

        // REPL循环
        Transaction currTransaction = null;
        Scanner inputScanner = new Scanner(this.in);
        String input;
        while (true) {
            try {
                input = bufferUserInput(inputScanner);
                if (input.length() == 0)
                    continue;
                if (input.startsWith("\\")) {
                    try {
                        parseMetaCommand(input, db);
                    } catch (Exception e) {
                        this.out.println(e.getMessage());
                    }
                    continue;
                }
                if (input.equals("exit")) {
                    throw new NoSuchElementException();
                }
            } catch (NoSuchElementException e) {
                // 用户发送了终止字符
                if (currTransaction != null) {
                    currTransaction.rollback();
                    currTransaction.close();
                }
                this.out.println("exit");
                this.out.println("再见！"); // 如果MariaDB这样说，我们也可以 :)
                return;
            }

            // 将输入转换为原始字节
            ByteArrayInputStream stream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
            RookieParser parser = new RookieParser(stream); // 查询解析器
            ASTSQLStatementList node;
            try {
                node = parser.sql_stmt_list(); // 生成SQL语法树
            } catch (ParseException | TokenMgrError e) {
                this.out.println("解析器异常: " + e.getMessage());
                continue;
            }
            StatementListVisitor visitor = new StatementListVisitor(db, this.out); // 创建一个语句列表访问者， 绑定了数据库和输出流
            try {
                node.jjtAccept(visitor, null); // 访问语法树
                currTransaction = visitor.execute(currTransaction); // 执行语句, 创建一个事务
            } catch (DatabaseException e) {
                this.out.println("数据库异常: " + e.getMessage());
            }
        }
    }

    public String bufferUserInput(Scanner s) {
        int numSingleQuote = 0;
        this.out.print("=> ");
        StringBuilder result = new StringBuilder();
        boolean firstLine = true;
        do {
            String curr = s.nextLine();
            if (firstLine) {
                String trimmed = curr.trim().replaceAll("(;|\\s)*$", "");
                if (curr.length() == 0) {
                    return "";
                } else if (trimmed.startsWith("\\")) {
                    return trimmed.replaceAll("", "");
                } else if (trimmed.toLowerCase().equals("exit")) {
                    return "exit";
                }
            }
            for (int i = 0; i < curr.length(); i++) {
                if (curr.charAt(i) == '\'') {
                    numSingleQuote++;
                }
            }
            result.append(curr);

            if (numSingleQuote % 2 != 0)
                this.out.print("'> ");
            else if (!curr.trim().endsWith(";"))
                this.out.print("-> ");
            else
                break;
            firstLine = false;
        } while (true);
        return result.toString();
    }

    private void printTable(String tableName) {
        TransactionContext t = TransactionContext.getTransaction();
        Table table = t.getTable(tableName);
        if (table == null) {
            this.out.printf("未找到表 \"%s\"。", tableName);
            return;
        }
        this.out.printf("表 \"%s\"\n", tableName);
        Schema s = table.getSchema();
        new PrettyPrinter(out).printSchema(s);
    }

    private void parseMetaCommand(String input, Database db) {
        input = input.substring(1); // 去掉开头的斜杠
        String[] tokens = input.split("\\s+");
        String cmd = tokens[0];
        TransactionContext tc = TransactionContext.getTransaction();
        if (cmd.equals("d")) {
            if (tokens.length == 1) {
                List<Record> records = db.scanTableMetadataRecords();
                new PrettyPrinter(out).printRecords(db.getTableInfoSchema().getFieldNames(),
                        records.iterator());
            } else if (tokens.length == 2) {
                String tableName = tokens[1];
                if (tc == null) {
                    try (Transaction t = db.beginTransaction()) {
                        printTable(tableName);
                    }
                } else {
                    printTable(tableName);
                }
            }
        } else if (cmd.equals("di")) {
            List<Record> records = db.scanIndexMetadataRecords();
            new PrettyPrinter(out).printRecords(db.getIndexInfoSchema().getFieldNames(),
                    records.iterator());
        } else if (cmd.equals("locks")) {
            if (tc == null) {
                this.out.println("没有持有锁，因为当前不在事务中。");
            } else {
                this.out.println(db.getLockManager().getLocks(tc));
            }
        } else {
            throw new IllegalArgumentException(String.format(
                "`%s` 不是有效的元命令",
                cmd
            ));
        }
    }

    private static String[] institution = {
            "berkeley", "berkley", "berklee", "Brocolli", "BeRKeLEy", "UC Zoom",
            "   UCB  ", "go bears", "   #1  "
    };

    private static List<String> startupMessages = Arrays
            .asList("与缓冲区管理器对话", "进行优雅的哈希",
                    "并行化停车空间", "批量加载考试准备",
                    "声明函数独立性", "维护长距离实体关系" );

    private static List<String> startupProblems = Arrays
            .asList("重建空气质量指数", "扑灭B+森林火灾",
                    "从PG&E停电中恢复", "消毒用户输入", "原地希尔排序",
                    "分发口罩", "加入Zoom会议", "缓存退出股市",
                    "建议事务自我隔离", "调整检疫优化器");

    private void startup() {
        Collections.shuffle(startupMessages);
        Collections.shuffle(startupProblems);
        this.out.printf("启动RookieDB (v%d.%d.%d-%s)\n", VERSION[0], VERSION[1], VERSION[2], LABEL);
        sleep(100);
        for (int i = 0; i < 3; i++) {
            this.out.print(" > " + startupMessages.get(i));
            ellipses();
            sleep(100);
            if (i < 4) {
                this.out.print(" 完成");
            } else {
                ellipses();
                this.out.print(" 错误！");
                sleep(125);
            }
            sleep(75);
            this.out.println();
        }
        this.out.println("\n遇到意外问题！应用修复：");
        sleep(100);
        for (int i = 0; i < 3; i++) {
            this.out.print(" > " + startupProblems.get(i));
            ellipses();
            this.out.print(" 完成");
            sleep(75);
            this.out.println();
        }
        sleep(100);
        this.out.println();
        this.out.println("初始化成功！");
        this.out.println();
    }

    private void ellipses() {
        for (int i = 0; i < 3; i++) {
            this.out.print(".");
            sleep(25 + this.generator.nextInt(50));
        }
    }

    private void sleep(int timeMilliseconds) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeMilliseconds);
        } catch (InterruptedException e) {
            this.out.println("收到中断信号。");
        }
    }
}
