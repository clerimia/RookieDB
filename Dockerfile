# 运行时镜像
FROM eclipse-temurin:11-jdk-jammy
WORKDIR /app

# 复制本地构建好的 JAR 文件
COPY ./target/*.jar /app/rookiedb.jar

# 暴露服务器端口
EXPOSE 18600

# 设置环境变量
ENV JAVA_OPTS="-Xms256m -Xmx512m"

# 启动数据库服务器
CMD ["sh", "-c", "java $JAVA_OPTS -cp rookiedb.jar edu.berkeley.cs186.database.cli.Server"]