# 第3章 构建分类模型

## 3.1 分类模型概述

MLlib中：

* 二分类的模型：SVM，逻辑回归，决策树，随机森林，梯度提升树，朴素贝叶斯
* 多分类的模型：逻辑回归，决策树，随机森林,朴素贝叶斯

## 3.2 分类模型算法

MLlib中损失函数：

* 折页损失：hinge loss  --svm
* 逻辑损失：logistic loss  -- 逻辑回归
* 平方损失：squared loss

MLlib中正则项：

* L1
* L2
* Elasitic Net

MLlib中优化方法:

* 随机剃度下降SGD
* 改进的拟牛顿法L-BFGS

### 3.2.1 逻辑回归

实际中特征规模很大时，使用L-BFGS加快求解

### 3.2.2 朴素贝叶斯

### 3.2.3 SVM

### 3.2.4 决策树

### 3.2.5 K-近邻

## 3.3 分类效果评估

### 3.3.1 正确率

### 3.3.2 准确率，召回率和F1值

### 3.3.3 ROC和AUC

## 3.4 App数据的分类实现

### 3.4.1 选择分类器

### 3.4.2 准备数据

### 3.4.3 训练模型

### 3.4.4 模型性能评估

## 3.5 其它分类模型

### 3.5.1 随机森林

### 3.5.2 梯度提升树

### 3.5.3 因式分解机
# 代码实战
1. zipWithIndex
```
scala> var rdd2 = sc.makeRDD(Seq("A","B","R","D","F"),2)
rdd2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[34] at makeRDD at :21
 
scala> rdd2.zipWithIndex().collect
res27: Array[(String, Long)] = Array((A,0), (B,1), (R,2), (D,3), (F,4))
```
2. flatMap 可理解为两次map
```
val arr=sc.parallelize(Array(("A",1),("B",2),("C",3)))
arr.map(x=>(x._1+x._2)).foreach(println)
输出：
A1
B2
C3
```