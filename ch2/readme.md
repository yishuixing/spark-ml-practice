## ch2 数据分析流程和方法

1. windows下运行 错误：Exception in thread "main" java.lang.IllegalArgumentException: requirement failed: Nothing has been added to this summarizer

   解决： input 数据为空 造成的

2. windows运行结果：

   ```
   Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
   19/10/22 11:09:53 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
   19/10/22 11:09:55 INFO FileInputFormat: Total input paths to process : 1
   19/10/22 11:09:55 INFO deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
   19/10/22 11:10:26 INFO FileOutputCommitter: Saved output of task 'attempt_20191022110955_0008_m_000000_0' to file:/F:/bd/code/spark-ml-practice/ch2/output/_temporary/0/task_20191022110955_0008_m_000000
   Mean[60.16221566503564,25.30645613117692,37.176763903933015]
   Variance[18547.429811930655,1198.6307291577361,7350.7365871949905]
   NumNonzeros[107092.0,107092.0,107092.0]
   19/10/22 11:10:34 WARN Executor: Managed memory leak detected; size = 59880284 bytes, TID = 28
   19/10/22 11:10:49 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
   19/10/22 11:10:49 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
   correlMatrix1.0                 0.7329442022276709  0.9324997691135504  
   0.7329442022276709  1.0                 0.5920355112372706  
   0.9324997691135504  0.5920355112372706  1.0   
   ```

3. 代码阅读

   * map: 比如一个partition中有1万条数据；那么你的function要执行和计算1万次。

     **MapPartitions**:  假设一个rdd有10个元素，分成3个分区 ，其输入函数会只会被调用3次，每个分区调用1次 。

      map的输入变换函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区 

     如果在map过程中需要频繁创建额外的对象(例如将rdd中的数据通过jdbc写入数据库,map需要为每个元素创建一个链接而mapPartition为每个partition创建一个链接),则mapPartitions效率比map高的多。

     SparkSql或DataFrame默认会对程序进行mapPartition的优化， 但是MapPartitions操作，对于大量数据来说，比如甚至一个partition，100万数据，一次传入一个function以后，那么可能一下子内存不够，但是又没有办法去腾出内存空间来，可能就OOM，内存溢出 

   * reduceByKey  相同的key执行reduce
   
     ```
     reduceByKey 接收两个同类型参数，返回一个同类型参数
     reduceByKey{
     	case(a,b)=>a+b
     }
     reduceByKey((a,b)=>a+b)   和上面相同的功能
     eg: {(1, 2), (3, 4), (3, 6)}
     rdd.reduceByKey( (x, y) => x + y)  结果： {(1, 2), (3, 10)}
     ```
   
   * map-case
   
     ```
     map{
     case (a)=>b*2
     }
     等于 map((a)=>b*2)
     ```
   
   *  Spark MLlib中支持两种类型的矩阵，分别是密度向量（Dense Vector）和稀疏向量（Spasre Vector），密度向量会存储所有的值包括零值，而稀疏向量存储的是索引位置及值，不存储零值，在数据量比较大时，稀疏向量才能体现它的优势和价值 
   
   *  宽依赖就是宽依赖是指子RDD的分区依赖于父RDD的多个分区或所有分区 
   
      窄依赖是指父Rdd的分区最多只能被一个子Rdd的分区所引用 
   
      两种方法是可以重设RDD分区：分别是coalesce()方法和repartition() 
   
   * 两组数据相关关系计算（Pearson、Spearman）

# 书上的笔记

## 数据分析流程和方法

### 流程：

1. 业务调研：问题是什么?问题背后的原因？是否可行？怎样获取数据？
2. 明确目标：量化目标
3. 数据准备：数据预处理ETL,通过spark sql完成
4. 特征处理：
   * 特征向量化
   * 文本特征处理：
     * 分词:Tokenization,RegexTokenizer
     * 去停用词:StopWordsRemover
     * 词稀疏编码:StringIndexer,Ngram,TF/IDF,word2vec
   * 特征预处理:
     * 特征归一化:StandardScaler ,MinMaxScaler,MaxAbsScaler
     * 正则化: Normalizer
     * 二值化: Binarizer
5. 模型训练与评估
   * 准备数据集: 70%训练，30%测试
   * 选适当的建模技术:
     * 汇总统计: 加和，计数，均值，标准差，中位数，众数，四分位数，最大值，最小值
     * 对比分析：卡方检验，方差分析
     * 趋势分析：回归
     * 分布分析：统计分布
     * 因子分析：多元线性回归
     * 聚类分析：分层聚类分析，LDA主题模型
   * 建立模型
   * 模型评估：准确率，召回率
6. 输出结论

### 方法

1. 汇总统计
   * 分布度量：概率分布表，频率表，直方图
   * 频度度量：众数
   * 位置度量：均值，中位数
   * 散度度量：极差，方差，标准差
   * 多元比较：相关系数
   * 模型评估：准确率，召回率
2. 相关性分析
   * 皮尔逊Pearson：正态分布的数据
   * 斯皮尔曼Spearman：不符合正态分布的数据
3. 分层抽样
   * sampleByKey
   * sampleByKeyExact
4. 假设检验：皮尔林卡方检验  Statistics中chiSqTest