package anjun

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession
import anjun.spark.ml.practice.util.{AppConst, FileUtils}

/**
  * whdir: omitted.
  * input: libsvm file.
  * output: model store path.
  * mode: yarn-client, yarn-cluster or local[*].
  * spark-warehouse 2rd_data/ch03/libsvm/part-00000 output/ch03/model local[2]
  */

object AppClassification {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//参数 workdir output/ch3/libsvmdata/part-00000 output/ch3/model "local[*]"
    val Array(whdir, input, output, mode) = args
    FileUtils.dirDel(output)
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", whdir)
      .master(mode)
      .appName(this.getClass.getName)
      .getOrCreate()

    val data = spark.read.format("libsvm").load(input)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model.
    val model = new NaiveBayes().fit(trainingData)
    val predictions = model.transform(testData)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy: " + accuracy)
     /**
     |-- label: double (nullable = true)
     |-- features: vector (nullable = true)
     |-- rawPrediction: vector (nullable = true)
     |-- probability: vector (nullable = true)
     |-- prediction: double (nullable = true)
       */
    model.save(output)
    spark.stop()
  }
}