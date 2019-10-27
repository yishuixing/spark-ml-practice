import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object UserDraw {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //2rd_data/ch09/action.txt 2rd_data/ch09/document.txt output/ch10 local[2]
    val Array(actionPath,documentPath,output,mode) = args

    // 基础标签
    basicTag(actionPath,output+"/basicTag",mode)
    // 兴趣标签
    interestTag(documentPath,actionPath,output+"/interestTag",mode)
  }

  // 数据格式：userid~ docid ~behaivor~time~ip
  //160520092238579653~160704235940001~0~20160705000040909~1.49.185.165
  //160520092238579653~160704235859003~0~20160705000040909~1.49.185.165

  /** *
    * 读取用户行为数据，计算用户基础标签，包括点击数、浏览数和点击率
    * actionPath action.txt
    * output  输出路径
    */
  def basicTag(actionPath: String, output: String,mode: String) = {
    val conf = new SparkConf()
      .setMaster(mode)//.setMaster("local")
      .setAppName("User Draw")
    val sc = new SparkContext(conf)

    val data = sc.textFile(actionPath)
    val basicTags = data
      .map(x => x.split("~"))
      .map(x => (x(0),x(2).toInt))
      .map(x => (x,1))
       // 统计用户的各类行为数
      .reduceByKey(_+_)
      .map{
        case((userId,behavior),cnt) => (userId,(behavior,cnt))
      }
       //合并一个用户的信息，并计算点击率
      .groupByKey().map{
      case(userId,tags) =>
        val tg = tags.toMap
        val click = tg.get(1).getOrElse(0)
        val show = tg.get(0).getOrElse(0)
        val rate = if(show > 0){
          click.toDouble / show
        }else{0}
        (userId,show,click,rate)
    }
    basicTags.repartition(1).saveAsTextFile(output)
    sc.stop()
  }

  //数据格式：docid ~ channelname ~ source ~ keyword:score
  //160705131650005~科技~偏执电商~支付宝:0.17621 医疗:0.14105 复星:0.07106 动作:0.05235 邮局:0.04428
  //160705024106002~体育~平大爷的刺~阿杜:0.23158 杜兰特:0.09447 巨头:0.08470 拯救者:0.06638 勇士:0.05453
  /**
    * 读取用户行为数据和新闻标签数据，计算用户频道兴趣标签
    * documentInput document.txt
    * actionInput action.txt
    * output 输出路径
    */
  def interestTag(documentInput: String, actionInput: String, output: String, mode: String) = {
    val conf = new SparkConf()
      .setMaster(mode)//.setMaster("local")
      .setAppName("User Draw")
    val sc = new SparkContext(conf)

    // 新闻标签导入，并用map存储，用户后续查询
    val sources = sc.textFile(documentInput)
      .map(x => x.split('~'))
      .filter(x=>x.length>2)
      .map(x => (x(0), x(2))) //docid source
      .collect().toMap

    // 读入用户行为，只要点击日志
    val actions = sc.textFile(actionInput)
      .map(x => x.split('~'))
      .filter(x => ((x(2).toInt) == 1))
      .map(x => (x(0), x(1))) //userid~ docid

    // 查询新闻标签，并进行统计
    val interestTags = actions.map{
      case (userId, itemid) =>
        val source = sources.get(itemid).getOrElse(-1)
        (userId, source)}
      .filter(x => x._2 != -1)
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .map{
        case ((userId, source), cnt) => (userId, (source, cnt))
      }
      .groupByKey()
    interestTags.repartition(1).saveAsTextFile(output)
    sc.stop()
  }
}
