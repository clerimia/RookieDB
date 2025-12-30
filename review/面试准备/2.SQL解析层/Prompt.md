1. 这个层次使用了jjt对于数据库的SQL语句已经一些命令解析进行了词法设计
2. 通过jjt自动创建了 /cli/parser 用于解析用户的SQL语句创建对应的语法解析树
3. 使用visitor模式遍历这个语法解析树
   - 首先CLIInterface解析SQL语句生成对应的语法树
   - 然后CLIInterface创建一个空的StatementListVisitor
   - CLIInterface调用node.jjtAccpet() 访问语法树，创建对应节点的visitor填充StatementListVisitor的 visitorList
   - CLIInterface调用StatementListVisitor的excute()
   - 然后遍历这个visitorList 调用对应的visitor() 方法开始执行SQL语句，进入下一层：SQL优化与执行层
4. 你可以帮我描述一下各个Visitor的作用