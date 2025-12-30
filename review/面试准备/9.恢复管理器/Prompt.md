恢复管理器
1. 这个是Project5部分，我主要负责实现ARIESRecoveryManager的以下内容:
   - 正向处理阶段
     - 事务状态相关 commit()、abort()、end()、rollbackToLSN()
     - 日志记录相关 logPageWrite()、
     - 保存点相关   rollbackToSavePoint()
     - 检查点      checkPoint()
   - 重启恢复阶段
     - restart()
     - restartAnalysis()
     - restartRedo()
     - restartUndo()
2. 也就是基于框架实现了ARIES三阶段恢复算法
3. 同样不够深入，容易被问住