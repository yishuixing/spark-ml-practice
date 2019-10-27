import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

/**
  * Created by seawalker on 2016/11/20.
  *  用户标识ID 电影标识ID集合
  * 136547	93,136,21,73,76
  */
object MoviesFPBatch {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //spark-warehouse 2rd_data/ch06/ar/part-00000 local[2]
    val Array(whdir,input,mode) = args
    val conf = new SparkConf()
      .set("spark.sql.warehouse.dir", whdir)
      .setMaster(mode)
      .setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)
    val transactions = sc.textFile(input).map(_.split("\t")).map(_(1).split(","))
    transactions.cache()

    val minSupports = Seq(0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05)
    val minConfidences = Seq(0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4)

    val cnts = minSupports.flatMap {
      case minSupport =>
        val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(4)
        val model = fpg.run(transactions)
        minConfidences.map{
          case minConfidence =>
            val rulesCnt = model.generateAssociationRules(minConfidence).count()
            (minSupport, minConfidence, rulesCnt)
        }
    }.map {
      case (minSupport, minConfidence, rulesCnt) => s"$minSupport\t$minConfidence\t$rulesCnt"
    }

    cnts.foreach(println)
    transactions.unpersist()
    sc.stop()
  }
}
