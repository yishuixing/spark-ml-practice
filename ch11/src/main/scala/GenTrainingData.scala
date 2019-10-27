import org.apache.spark.sql.{SparkSession}

//action：userid~ docid ~behaivor(label)~time~ip
//160520092238579653~160704235940001~0~20160705000040909~1.49.185.165
//160520092238579653~160704235859003~0~20160705000040909~1.49.185.165
//define case class for action data
case class Action(docid: String, label:Int)

//document：docid ~ channelname ~ source ~ keyword:score
//160705131650005~科技~偏执电商~支付宝:0.17621 医疗:0.14105 复星:0.07106 动作:0.05235 邮局:0.04428
//160705024106002~体育~平大爷的刺~阿杜:0.23158 杜兰特:0.09447 巨头:0.08470 拯救者:0.06638 勇士:0.05453
//define case class for document data
case class Dccument(docid: String, channal: String, source: String, tags: String)

object GenTrainingData {
  def main(args: Array[String]): Unit = {

    //2rd_data/ch09/action.txt 2rd_data/ch09/document.txt output/ch11 local[2]
    val Array(actionPath, documentPath, output, mode) = args
    // 创建Spark实例
    val spark = SparkSession.builder
      .master(mode)
      .appName(this.getClass.getName)
      .getOrCreate()

    import spark.implicits._
    val ActionDF = spark.sparkContext.textFile(actionPath).map(_.split("~"))
      .map(x => Action(x(1).trim.toString, x(2).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    //ActionDF.createOrReplaceTempView("actiondf")

    val documentDF = spark.sparkContext.textFile(documentPath).map(_.split("~")).filter(_.length > 3)
      .map { case x =>
        val xtags = x(3).split(" ").filter(_.length > 0).map { b => b.substring(0, b.indexOf(":")) }.mkString("|")
        Dccument(x(0).trim.toString, x(1).trim.toString, x(2).trim.toString, xtags.toString)
      }
      .toDF()
    // Register the DataFrame as a temporary view
    //documentDF.createOrReplaceTempView("documentdf")

    // 将查询结果放到tempDF中，完成dataframe转化
    //val tempDF = spark.sql("select actiondf.docid,actiondf.label,documentdf.channal,documentdf.source,documentdf.tags from actiondf,documentdf where actiondf.docid = documentdf.docid")
    val tempDF = documentDF.join(ActionDF, documentDF("docid").equalTo(ActionDF("docid")))
    //tempDF.select($"tags").show(100)

    // 编码格式转换
    val minDF = tempDF.select($"tags").rdd
      .flatMap{ x => x.toString.replace("[","").replace("]","").split('|') }.distinct
    //minDF.coalesce(1).saveAsTextFile(output+"/tags")
    val indexes = minDF.collect().zipWithIndex.toMap
    println(indexes.toList.length) //23937
    //
    val libsvmDF = tempDF.select($"label", $"tags").map {
      x =>
        val label = x(0)
        val terms = x(1).toString.replace("[","").replace("]","")
          .split('|') //使用单引号
          .map(v => (indexes.get(v).getOrElse(-1)+1, 1)) //索引从0开始
          .sortBy(_._1) //libsvm 需要升序
          .map(x => x._1 + ":" + x._2)
          .mkString(" ")
        (label.toString + " " + terms)
    }
    libsvmDF.show(100)

    //保存模型时存在：Exception while deleting local spark dir，不影响结果生成，作为已知问题暂时搁置。
    //libsvmDF.coalesce(1).write.format("text").save(output+"/model")
    //libsvmDF.rdd.coalesce(1).saveAsTextFile(output+"/model")
    val Array(trainingdata, testdata) = libsvmDF.randomSplit(Array(0.7, 0.3))
    trainingdata.rdd.coalesce(1).saveAsTextFile(output+"/training")
    testdata.rdd.coalesce(1).saveAsTextFile(output+"/test")
    //
    //spark.stop()
  }
}
