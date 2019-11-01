#  第8章
主成分分析（PCA）是一种统计方法，用于查找旋转，以使第一个坐标具有最大的方差，而每个后续坐标又具有最大的方差。旋转矩阵的列称为主成分。PCA被广泛用于降维
spark.mllib supports PCA for tall-and-skinny matrices stored in row-oriented format and any Vectors.
### 补充知识：
    spark.mllib中的算法接口是基于RDDs的；
    spark.ml中的算法接口是基于DataFrames的
    大体概念：DataFrame => Pipeline => A new DataFrame
    Pipeline: 是由若干个Transformers和Estimators连起来的数据处理过程
    Transformer：入：DataFrame => 出： Data Frame
    Estimator：入：DataFrame => 出：Transformer
    参考 
    https://spark.apache.org/docs/latest/ml-guide.html
    https://spark.apache.org/docs/latest/mllib-guide.html
### RDD和DataFrame
* DataFrame多了数据的结构信息，即schema
* DataFrame除了提供了比RDD更丰富的算子以外，更重要的特点是提升执行效率、减少数据读取以及执行计划的优化，比如filter下推、裁剪等
### RDD和DataSet
* DataSet以Catalyst逻辑执行计划表示，并且数据以编码的二进制形式被存储，不需要反序列化就可以执行sorting、shuffle等操作
* DataSet创立需要一个显式的Encoder，把对象序列化为二进制，可以把对象的scheme映射为Spark SQl类型，然而RDD依赖于运行时反射机制
* DataSet的性能比RDD的要好很多
### DataFrame和DataSet
Dataset可以认为是DataFrame的一个特例，主要区别是Dataset每一个record存储的是一个强类型值而不是一个Row。因此具有如下三个特点：
* DataSet可以在编译时检查类型
* 面向对象的编程接口。
* 后面版本DataFrame会继承DataSet，DataFrame是面向Spark SQL的接口
DataFrame和DataSet可以相互转化，df.as[ElementType]这样可以把DataFrame转化为DataSet，ds.toDF()这样可以把DataSet转化为DataFrame
