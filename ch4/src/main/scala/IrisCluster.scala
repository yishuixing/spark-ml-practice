import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.SparkSession

/**
  * Created by seawalker on 2016/11/18.
  */

object IrisCluster {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //spark-warehouse 2rd_data/ch04/iris_kmeans.txt local[2]
    val Array(whdir,input,mode) = args
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", whdir)
      .master(mode)
      .appName(this.getClass.getName)
      .getOrCreate()
    //import spark.implicits._
    import spark.implicits._

    // Load data.
    val dataset = spark.read.format("libsvm").load(input)
    // MinMaxScaler
    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(dataset)
    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dataset)

    //
    val featuresCol = "scaledFeatures"
    val k = 3
    val model = new KMeans().setK(k).setFeaturesCol(featuresCol).setSeed(1L).fit(scaledData)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    //集内 平方误差和（Within Set Sum of Squared Error，WSSSE）
    //误差平方和（Sum of Squares Error，SSE）
    val WSSSE = model.computeCost(scaledData)
    println(s"$k: Squared Errors = $WSSSE")

    model.clusterCenters.foreach(println)

    // prediction
    model.setFeaturesCol(featuresCol)
      .transform(scaledData)
      .groupBy($"label", $"prediction")
      .count()
      .show()

    spark.stop()
  }
}