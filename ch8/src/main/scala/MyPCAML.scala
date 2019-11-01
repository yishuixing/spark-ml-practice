import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于spark.ml 新api
  */
object MyPCAML {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("MyPCAML")
      .master("local[*]")
      .getOrCreate();
    val rawDataFrame: Dataset[Row] = spark.read.format("libsvm")
      .load("data/mllib/sample_libsvm_data.txt")
    //首先对特征向量进行标准化
    val scaledDataFrame: Dataset[Row] = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(false) //对于稀疏数据（如本次使用的数据），不要使用平均值
      .setWithStd(true)
      .fit(rawDataFrame)
      .transform(rawDataFrame);
    //PCA Model
    val pcaModel: PCAModel = new PCA()
      .setInputCol("scaledFeatures")
      .setOutputCol("pcaFeatures")
      .setK(3) //
      .fit(scaledDataFrame);
    //进行PCA降维
    pcaModel.transform(scaledDataFrame).select("label", "pcaFeatures").show(100, false);

    spark.close()
  }
}
