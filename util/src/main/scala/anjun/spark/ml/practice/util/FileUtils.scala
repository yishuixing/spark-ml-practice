package anjun.spark.ml.practice.util

import java.io.File

object FileUtils {
  //删除目录和文件
  def dirDel(f: String) {
    val path = new File(f)
    if (!path.exists())
      return
    else if (path.isFile()) {
      path.delete()
      println(path + ":  文件被删除")
      return
    }

    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d.getAbsolutePath)
    }

    path.delete()
    println(path + ":  目录被删除")

  }
}
