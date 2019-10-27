import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
/**
  * Created by seawalker on 2016/11/22.
  */
object DigitPCA {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //2rd_data/ch08/train.dat 2rd_data/ch08/test.dat local[2]
    val Array(trainPath,testPath,mode) = args
    val conf = new SparkConf()
      .setMaster(mode)
      .setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val data: RDD[LabeledPoint] = sc.textFile(trainPath).map(_.split(" ")).map
    {  terms =>
        val label = terms(0).toInt
        new LabeledPoint(label, Vectors.dense(terms(1).split("").map(_.toDouble)))
    }.filter(_.label <= 2.0)

    val k = 3
    // Compute the top k principal components.
    val pca = new PCA(k).fit(data.map(_.features))
    // Project vectors to the linear space spanned by the top k principal
    // components, keeping the label
    val projected = data.map(p => p.copy(features = pca.transform(p.features)))
    projected.take(10).foreach(println)

    sc.stop()
  }
}
