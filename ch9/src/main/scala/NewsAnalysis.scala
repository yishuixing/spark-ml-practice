import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object NewsAnalysis {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length < 2) {
      System.err.println("Usage:NewsAnalysis <actionInPath> <outPut> <mode>")
      System.exit(1)
    }

    //2rd_data/ch09/action.txt output/ch09 local[2]
    val Array(actionInPath,output,mode) = args
    //
    val conf = new SparkConf()
      .setMaster(mode)
      .setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    //1.action ―― UID+DOCID+0/1（曝光/点击）+时间+ip
    //160520092238579653~160704235940001~0~20160705000040909~1.49.185.165
    //160520092238579653~160704235859003~0~20160705000040909~1.49.185.165
    // 处理曝光、点击日志数据，解析有效数据
    val actionLog = sc.textFile(actionInPath).map{
      case line => {
        val Array(uid,docId,tag,time,ip) = line.split("~",-1)
        (uid,docId,tag,time,ip)
      }
    }.cache()

    // 保存解析好的action日志
    actionLog.saveAsTextFile(output+"/actionLog")
    // 过滤异常点击，只有当天曝光的点击才有效
    val validActionLog = clickFilter(actionLog)

    //日活跃统计、异常计算
    dayStat(validActionLog,output+"/dayActive")
    sc.stop()
  }

  /**
    * 点击过滤, 当天曝光的点击才有效
    */
  def clickFilter(allClickLog:RDD[(String,String,String,String,String)])
  :RDD[(String,String,String,String)] = {
    val validClick = allClickLog.map
    {
      case (uid,docId,action,time,ip) =>
        //取时间前8位，如20160705000040909的前8位20160705，并将(uid,docId,time)设为主键，进行分组
        ((uid,docId,time.substring(0,8)),action)
    }
      .groupByKey()
      .filter{
        case ((uid,docId,date),iter) => {
          val tmp = iter.toSet
          tmp.contains("0")//保留数据集中有曝光的记录
        }
      }
      .flatMap{
        case ((uid,docId,date),iter) => iter.map{
          case (action) => (uid,docId,date,action)
        }
      }
    validClick
  }

  /**
    * 日活跃、有效点击率
    */
  def dayStat(validClick:RDD[(String,String,String,String)],
              dayStatPath:String):Unit = {

    // 日活跃计算
    val dayActive = validClick.map{
      case (uid,docId,date,actionTag) => (date,uid)
    }
      .distinct()
      .map{
        case (date,uid) => (date,1)
      }
      .reduceByKey(_ + _)
      .cache()

    //输出每日活跃用户数
    dayActive.saveAsTextFile(dayStatPath)

    //转换数据格式
    val dayActiveData = dayActive.map{
      case(date,dayActives)=>Vectors.dense(dayActives.toDouble)
    }

    //统计方法
    val summary: MultivariateStatisticalSummary = Statistics.colStats(dayActiveData)
    //均值、方差
    println("Mean:"+summary.mean)
    println("Variance:"+summary.variance)
    }
  }
