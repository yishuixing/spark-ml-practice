package anjun

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vectors, _}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat

/**
  * Created by haixiang on 2016/4/9.
  * exploring the gowalla data set.
  * download url: https://snap.stanford.edu/data/loc-gowalla.html
  * https://snap.stanford.edu/data/loc-gowalla_totalCheckins.txt.gz
  * format: [user]	[check-in time]	[latitude] [longitude] [location id]
  * 4913	2009-12-13T18:01:14Z	41.9759716333	-87.90606665	165768
  * 4913	2009-12-13T18:01:02Z	41.97656125	-87.9064951333	49205
  */
// 定义数据类
case class CheckIn(user: String, time: String, latitude: Double, longitude: Double, location: String)

// 创建实例
object GowallaDatasetExploration {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  //数据下载 https://snap.stanford.edu/data/loc-gowalla_totalCheckins.txt.gz
    //参数  data/ch2/loc-gowalla_totalCheckins.txt  output/ch2  "local[*]"
    val Array(input, output, mode) = args
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val sc = new SparkContext(conf)

    val gowalla = sc.textFile(input).map(_.split("\t")).mapPartitions {
      case iter =>
        //        val format = DateTimeFormat.forPattern("yyyy-MM-dd\'T\'HH:mm:ss\'Z\'")
        iter.map {
          //DateTime.parse(terms(1),format)
          case terms => CheckIn(terms(0), terms(1).substring(0, 10), terms(2).toDouble, terms(3).toDouble, terms(4))
        }
    }
    // user, checkins, check in days, locations
    val data = gowalla.map {
      case check: CheckIn => (check.user, (1L, Set(check.time), Set(check.location))) // Set(集合)是没有重复的对象集合，所有的元素都是唯一的。
    }.reduceByKey {
      // 并集 union
      /**reduceByKey 接收k,v对 ，对相同k的value作reduce,这里 取到的left,right 都是 value
        * left ,right 都像这这样(1,Set(2010-10-16),Set(453937)) 可以用下面语句打出来看
        */
//      case(left,right)=>{
//        println(left)
//        (left)
//      }
      case (left, right) => (left._1 + right._1, left._2.union(right._2), left._3.union(right._3))
    }.map {
      case (user, (checkins, days: Set[String], locations: Set[String])) =>
        Vectors.dense(checkins.toDouble, days.size.toDouble, locations.size.toDouble)
    }
    //保存
    //两种方法是可以重设RDD分区：分别是coalesce()方法和repartition()
    //coalesce()方法的作用是返回指定一个新的指定分区的RDD
    //如果是生成一个窄依赖的结果，那么不会发生shuffle。比如：1000个分区被重新设置成10个分区，这样不会发生shuffle
    data.coalesce(1).saveAsTextFile(output)

    //统计方法
    val summary: MultivariateStatisticalSummary = Statistics.colStats(data)
    //均值
    println("Mean" + summary.mean)
    //方差
    println("Variance" + summary.variance)
    //非零元素的目录
    println("NumNonzeros" + summary.numNonzeros)
    for(d <- data) {
      println(d)
    }
    //皮尔逊
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println("correlMatrix" + correlMatrix.toString)

    sc.stop()
  }
}
