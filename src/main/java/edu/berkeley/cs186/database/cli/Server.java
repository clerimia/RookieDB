package edu.berkeley.cs186.database.cli;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 警告：我们提供将RookieDB作为服务器运行的选项，仅仅是为了演示锁机制在多客户端环境下的工作原理。
 * 我们不建议将此服务器暴露到公共互联网上，因为没有内置的身份验证机制。
 * 即使您的实例中存储的是您不介意泄露的测试数据，提供的接口也允许对demo/目录中的文件进行几乎任意的写入访问。
 * 这意味着在最好的情况下，恶意实体可以随意占用您的磁盘空间，
 * 而在更坏的情况下，可能会用恶意文件填充您的磁盘。
 *
 * - 一位不想成为161案例研究的前任助教
 *
 * 要在服务器模式下使用RookieDB，请运行本文件中的main函数。
 * 这将在本地机器的18600端口启动一个服务器。
 * 然后，在项目根目录下运行`python client.py`（需要Python 3）
 *
 * 或者，使用像`netcat`或`nc`这样的工具来建立连接，例如：
 * - `netcat localhost 18600`
 * - `nc localhost 18600`（取决于netcat的安装方式）
 * - `ncat localhost 18600`（适用于Windows用户，可能需要先下载）
 */
public class Server {
    public static final int DEFAULT_PORT = 18600;

    private int port;

    public static void main(String[] args) {
        // 注意：在尝试运行之前，您可能需要先完成项目4。
        Database db = new Database("demo", 25, new LockManager());
        
        // 完成项目5（恢复）后使用以下代码
        // Database db = new Database("demo", 25, new LockManager(), new ClockEvictionPolicy(), true);

        Server server = new Server();
        server.listen(db);
        db.close();
    }

    class ClientThread extends Thread {
        Socket socket;
        Database db;

        public ClientThread(Socket socket, Database db) {
            this.socket = socket;
            this.db = db;
        }

        public void run() {
            PrintStream out;
            InputStream in;
            try {
                out = new PrintStream(this.socket.getOutputStream(), true);
                in = new BufferedInputStream(this.socket.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            try {
                CommandLineInterface cli = new CommandLineInterface(db, in, out);
                cli.run();
            } catch (Exception e) {
                // Fatal error: print stack trace on both the server and client
                e.printStackTrace();
                e.printStackTrace(out);
            } finally {
                try {
                    this.socket.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    public Server() {
        this(DEFAULT_PORT);
    }

    public Server(int port) {
        this.port = port;
    }

    public void listen(Database db) {
        try (ServerSocket serverSocket = new ServerSocket(this.port)) {
            while (true) {
                new ClientThread(serverSocket.accept(), db).start();
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + this.port);
            System.exit(-1);
        }
    }
}
