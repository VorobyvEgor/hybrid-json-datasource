package org.apache.spark.sql.hybrid

import java.io.{BufferedWriter, File, FileWriter}

object FileHelper {
  def getFiles(path: String): Array[File] = {
    val basePath = new File(path)

    if (!basePath.exists()) {
      return Array()
    }

    def getFilesRec(recPath: File): Array[File] = {
      if (recPath.isDirectory) {
        recPath.listFiles.flatMap(x => getFilesRec(x))
      }
      else {
        Array(recPath)
      }
    }

    val ret = getFilesRec(basePath)

    System.out.println(s"Files: ${ret.mkString("\n")}")

    ret
  }

  def write(path: String, data: Iterator[String]): Unit = {
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    while (data.hasNext) {
      bw.write(data.next())
      bw.write("\n")
    }
    bw.close()
  }

  def ensureDirectory(path: String): Unit = {
    this.synchronized {
      val dir = new File(path)
      dir.mkdir()
    }
  }
}