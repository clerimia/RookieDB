FROM maven:3.8.4-openjdk-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM openjdk:11-jre-slim
WORKDIR /app

# 安装 netcat 用于测试连接
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# 复制构建好的 JAR 文件
COPY --from=build /app/target/*.jar rookiedb.jar

# 创建 demo 目录用于存储数据库文件
RUN mkdir -p demo

# 暴露服务器端口
EXPOSE 18600

# 启动数据库服务器
CMD ["java", "-cp", "rookiedb.jar:.", "edu.berkeley.cs186.database.cli.Server"]