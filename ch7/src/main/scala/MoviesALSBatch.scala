import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by seawalker on 2016/11/19.
  */

object MoviesALSBatch {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //spark-warehouse 2rd_data/ch07/ratings.dat output/ch07/modelBest local[2]
    val Array(whdir, ratingsPath, output, mode) = args
    val spark = SparkSession.builder
      .config("spark.sql.warehouse.dir", whdir)
      .master(mode)
      .appName(this.getClass.getName)
      .getOrCreate()

    import spark.implicits._
    val ratings = spark.read.textFile(ratingsPath).map(parseRating).toDF()

    val Array(training, validation, test) = ratings.randomSplit(Array(0.6, 0.2, 0.2)).map(_.persist)
    val numTraining = training.count
    val numValidation = validation.count
    val numTest = test.count
    println(s"training: $numTraining, validation: $numValidation, test: $numTest.")

    val ranks = List(8, 10, 12, 16)
    val lambdas = List(0.01, 0.1, 1.0)
    val numIters = List(6, 8, 10)

    var bestModel: Option[ALSModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      // Build the recommendation model using ALS on the training data
      val als = new ALS()
        .setMaxIter(numIter)
        .setRank(rank)
        .setRegParam(lambda)
        .setUserCol("userId")
        .setItemCol("movieId")
        .setRatingCol("rating")
      val model: ALSModel = als.fit(training)

      // Evaluate the model by computing the RMSE on the validation data
      val validationRmse = computeRmse(model, validation)
      println(s"($rank, $lambda, $numIter)'s RMSE (validation): $validationRmse")

      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    // Evaluate the model by computing the RMSE on the test data
    val testRmse = computeRmse(bestModel.get, test)
    println(s"The best model was trained with ($bestRank,$bestLambda,$bestNumIter), and its RMSE(test) is $testRmse ")
    val baselineRmse = computeBaseline(training.union(validation), test, numTest)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
    //
    bestModel.get.save(output)
    spark.stop()
  }

  def computeRmse(model: ALSModel, df: DataFrame): Double = {
    val spark = df.sparkSession
    import spark.implicits._
    val predictions = model.transform(df)
      .filter($"prediction".notEqual(Double.NaN))
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    rmse
  }

  def computeBaseline( data: DataFrame, test: DataFrame, numTest:Long) : Double ={
    val spark = data.sparkSession
    import spark.implicits._
    data.createTempView("training")
    val meanRating = spark.sql("SELECT mean(rating) from training").collect()(0).getDouble(0)

    val baselineRmse = math.sqrt(test.map {
      case row => Math.pow(row.getFloat(2) - meanRating, 2)
    }.reduce(_ + _) / numTest)
    baselineRmse
  }

  def parseRating(str: String): Rating = {
    val fields = str.split(",")
    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }
}