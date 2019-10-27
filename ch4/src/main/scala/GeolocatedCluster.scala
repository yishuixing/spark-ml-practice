import anjun.spark.ml.practice.util.FileUtils
import breeze.linalg.DenseMatrix
import nak.cluster.DBSCAN._
import nak.cluster.GDBSCAN
import nak.cluster.Kmeans._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by haixiang on 2016/4/8.
 * Clustering geolocated data using Spark and DBSCAN
 * url https://www.oreilly.com/ideas/clustering-geolocated-data-using-spark-and-dbscan
 * [user]	[check-in time]	[latitude] [longitude] [location id]
 * 4913	2009-12-13T18:01:14Z	41.9759716333	-87.90606665	165768
 */

case class CheckIn(user: String, time: DateTime, latitude: Double, longitude: Double, location: String)

object GeolocatedCluster {
    def main(args: Array[String]){

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

      // data/ch4/loc-Gowalla_totalCheckins.txt output/ch4/dbscan "local[*]"
      val Array(input,output,mode) = args
      FileUtils.dirDel(output)
      val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
      val sc = new SparkContext(conf)

      val gowalla = sc.textFile(input).map(_.split("\t")).mapPartitions{
      case iter =>
        val format = DateTimeFormat.forPattern("yyyy-MM-dd\'T\'HH:mm:ss\'Z\'")
        iter.map{
          case terms => CheckIn(terms(0), DateTime.parse(terms(1),format), terms(2).toDouble, terms(3).toDouble,terms(4))
        }
    }

    val checkinsRDD = gowalla
      .map{case check => (check.user, (check.longitude, check.latitude))}
      .groupByKey()
      .mapValues(_.toArray)
      .map{
      case (user, points) =>
        val col1 = points.map(_._1)
        val col2 = points.map(_._2)
        val bdm = new DenseMatrix(points.size, 2, col1 ++ col2)
        (user, bdm)
    }

    val clustersRDD = checkinsRDD.mapValues(dbscan(0.01, 5, _))
    clustersRDD.foreach{
      case (uid, clusters) =>
        clusters.foreach{
        case cluster =>
          val id = cluster.id
          val points = cluster.points
          //println((id, points))
      }
    }
    clustersRDD.coalesce(1).saveAsTextFile(output)
    sc.stop()
  }

  /**
   * @param v points
   * @return clusters
   */
  def dbscan(epsilon:Double, minPoints:Int, v:breeze.linalg.DenseMatrix[Double]):scala.Seq[nak.cluster.GDBSCAN.Cluster[Double]] = {
    val gdbscan = new GDBSCAN(
      getNeighbours(epsilon, distance = euclideanDistance),
      isCorePoint(minPoints)
    )
    val clusters = gdbscan cluster v
    clusters
  }
}
