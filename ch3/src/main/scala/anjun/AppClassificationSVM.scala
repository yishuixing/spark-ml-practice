package anjun

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import anjun.spark.ml.practice.util.{AppConst, FileUtils}

/**
  * Created by seawalker on 2016/11/17.
  * input: libsvm file.
  * output: model store path.
  * mode: yarn-client, yarn-cluster or local[*].
  * spark-warehouse 2rd_data/ch03/libsvm/part-00000 output/ch03/svmmodel local[2]
  */
object AppClassificationSVM {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//workdir output/ch3/libsvmdata/part-00000 output/ch3/svmmodel "local[*]"
    val Array(wh, input, output, mode) = args
    FileUtils.dirDel(output)
    val sc = new SparkContext(new SparkConf()
                  .setAppName(this.getClass.getSimpleName)
                  .setMaster(mode)
                  .set("spark.sql.warehouse.dir", wh))

    /* 5 * 4 / 2 = 10 */
    val data = MLUtils.loadLibSVMFile(sc, input).cache()
    //combinations(n: Int): Iterator[List[A]] 取列表中的n个元素进行组合，返回不重复的组合列表，结果一个迭代器
    val labels = data.map(_.label).distinct().collect().sorted.combinations(2).map(x => (x.mkString("_"), x))

    labels.foreach {
      case (tag, tuple) =>
        val parts = data.filter(lp => tuple.contains(lp.label)).map
        {
          case lp =>
            val label = if (lp.label == tuple(0)) 0 else 1
            new LabeledPoint(label, lp.features) //标签向量
        }
        val splits = parts.randomSplit(Array(0.7, 0.3), seed = 11L)
        val training = splits(0).cache()
        val test = splits(1)
        val svmAlg = new SVMWithSGD()
           svmAlg.optimizer
          .setNumIterations(200)
          .setRegParam(0.01)
        val model = svmAlg.run(training)

        // Clear the default threshold.
        model.clearThreshold()
        val scoreAndLabels = test.map
        {
            point =>
            val score = model.predict(point.features)
            (score, point.label)
        }

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auc = metrics.areaUnderROC()
        //
        model.save(sc, output + tag)
    }
    sc.stop()
  }
}
