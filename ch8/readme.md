#  第8章
   
> PCA(Principal Component Analysis)，即主成分分析方法，是一种使用最广泛的数据降维算法。PCA的主要思想是将n维特征映射到k维上，这k维是全新的正交特征也被称为主成分，是在原有n维特征的基础上重新构造出来的k维特征。PCA的工作就是从原始的空间中顺序地找一组相互正交的坐标轴，新的坐标轴的选择与数据本身是密切相关的。其中，第一个新坐标轴选择是原始数据中方差最大的方向，第二个新坐标轴选取是与第一个坐标轴正交的平面中使得方差最大的，第三个轴是与第1,2个轴正交的平面中方差最大的。依次类推，可以得到n个这样的坐标轴。通过这种方式获得的新的坐标轴，我们发现，大部分方差都包含在前面k个坐标轴中，后面的坐标轴所含的方差几乎为0。于是，我们可以忽略余下的坐标轴，只保留前面k个含有绝大部分方差的坐标轴。事实上，这相当于只保留包含绝大部分方差的维度特征，而忽略包含方差几乎为0的特征维度，实现对数据特征的降维处理

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
