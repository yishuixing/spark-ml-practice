import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CleanCongestionData {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length < 2) {
      System.err.println("Usage:CleanCongestionData <InPath> <OutPut> <Model>")
      System.exit(1)
    }

    // 2rd_data/ch13/road_congestion_sample.txt output/ch13/CongestionModel local[2]
    val Array(input,output,mode) = args

    // 初始化SparkContext
    val conf = new SparkConf()
      .setMaster(mode)//.setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // 计算link的拥堵情况，指定道路、工作日状态、时间片时，link拥堵指数的平均值（四舍五入）取整，
    // key (linkid, work_flag, hour) value (congestion)
    // 85349482;1;20.5;1
    val data = sc.textFile(input).map(_.split(";"))
      .map {x => ((x(0),x(1),x(2)),x(3))}
      .groupByKey().mapValues(x=>{
        val a = x.toList.reduceLeft((sum,i)=>sum +i)//拥堵指数求和
        val b = x.toList.length
        Math.round(a.toInt/b)//平均拥堵指数
      })
    //data.coalesce(1).saveAsTextFile(output)

    // 根据key聚合数据后，使用hour 进行排序 并删除hour数据
    // key (linkid,work_flag, hour) value (congestion)->(linkid) value(work_flag,congestion)
    val collectData = data.sortBy(x=>x._1._3).map(x => ((x._1._1),(x._1._2+":"+x._2))).reduceByKey(_ + ";" + _)
    collectData.coalesce(1).saveAsTextFile(output)
  }
}
