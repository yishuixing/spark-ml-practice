# 第6章 构建关联规则模型
基础知识：
spark广播变量
* Spark 目前只支持 broadcast 只读变量
* broadcast 到节点而不是 broadcast 到每个 task？
因为每个 task 是一个线程，而且同在一个进程运行 tasks 都属于同一个 application。因此每个节点（executor）上放一份就可以被所有 task 共享
