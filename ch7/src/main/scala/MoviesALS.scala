import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by seawalker on 2016/11/19.
  */

case class Rating(userId: Int, movieId: Int, rating: Float)

object MoviesALS {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //spark-warehouse 2rd_data/ch07/ratings.dat output/ch07/model local[2]
    val Array(whdir, ratingsPath, output, mode) = args
    val spark = SparkSession.builder
      .config("spark.sql.warehouse.dir", whdir)
      .master(mode)
      .appName(this.getClass.getName)
      .getOrCreate()

    //import spark.implicits._
    import spark.implicits._

    val ratings = spark.read.textFile(ratingsPath).map(parseRating).toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    println(test.count())

    val rank = 6
    val iter = 5
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(iter)
      .setRank(rank)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model: ALSModel = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val rmse1 = evaluate(model, training)
    val rmse2 = evaluate(model, test)

    println(rmse1, rmse2)
    model.save(output)
    spark.stop()
  }

  def evaluate(model: ALSModel, df: DataFrame):(Long, Double) = {
    import df.sparkSession.implicits._
    val predictions = model.transform(df).filter($"prediction".notEqual(Double.NaN))
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    (predictions.count(), rmse)
  }

  def parseRating(str: String): Rating = {
    // 用户标识ID  电影标识ID 评分
    // 1108242,10756728,3
    val fields = str.split(",")
    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }
}