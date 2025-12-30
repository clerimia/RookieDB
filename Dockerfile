# 多阶段构建 - 构建阶段
FROM maven:3.8.4-openjdk-11 AS build
WORKDIR /app

# 复制项目文件
COPY pom.xml .
COPY RookieParser.jjt .
COPY parsergen.sh .
COPY src ./src

# 生成解析器代码（JavaCC）
RUN chmod +x parsergen.sh && ./parsergen.sh

# 编译并打包项目
RUN mvn clean package -DskipTests

# 运行时镜像
FROM openjdk:11-jre-slim
WORKDIR /app

# 安装 netcat 用于客户端连接测试
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# 从构建阶段复制编译好的 JAR 文件
COPY --from=build /app/target/*.jar /app/rookiedb.jar

# 复制客户端工具
COPY client.py /app/client.py

# 创建数据库数据目录
RUN mkdir -p /app/demo

# 暴露服务器端口
EXPOSE 18600

# 设置环境变量
ENV JAVA_OPTS="-Xms256m -Xmx512m"

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD nc -z localhost 18600 || exit 1

# 启动数据库服务器
CMD ["sh", "-c", "java $JAVA_OPTS -cp rookiedb.jar edu.berkeley.cs186.database.cli.Server"]
