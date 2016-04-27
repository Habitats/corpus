package no.habitats.corpus.common

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.apache.spark.Logging
import org.slf4j.MarkerFactory

object Log extends Logging {
  val marker = MarkerFactory.getMarker("CORPUS")

  def resultsFile(name: String) = {
    new File(Config.cachePath + "res/").mkdirs()
    val resultsFile = new File(s"${Config.dataPath}res/$name")
    resultsFile.getParentFile.mkdirs
    if (!resultsFile.exists) {
      i(s"Creating results file at ${resultsFile.getAbsolutePath} ...")
      resultsFile.createNewFile
    }
    resultsFile
  }

  private def writeLine(m: String, file: File) = {
    val writer = new PrintWriter(new FileOutputStream(file, true))
    writer.println(m)
    writer.close()
  }

  private def writeLines(m: Seq[String], file: File) = {
    val writer = new PrintWriter(new FileOutputStream(file, true))
    m.foreach(writer.println)
    writer.close()
  }

  def toFile(m: String, fileName: String) = {
    val resultsFile = new File(Config.dataPath + f"/$fileName")
    if (!resultsFile.exists) {
      Log.i(s"Creating custom file at ${resultsFile.getAbsolutePath} ...")
      resultsFile.getParentFile.mkdirs()
      resultsFile.createNewFile()
    }
    writeLine(m.toString, resultsFile)
  }

  def toFile(m: Traversable[String], fileName: String, path: String = Config.dataPath) = {
    val resultsFile = new File(path + f"/$fileName")
    Log.i(s"Creating custom file at ${resultsFile.getAbsolutePath} ...")
    FileUtils.deleteQuietly(resultsFile)
    resultsFile.createNewFile
    writeLine(m.mkString("\n"), resultsFile)
  }

  def init() = {
    writeLine("", resultsFile(Config.resultsFileName))
  }

  def f(m: Any): String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " > " + m

  def i(m: Any) = log(m)

  def r(m: Any) = {
    i(m)
    writeLine(f(m), resultsFile(Config.resultsFileName))
  }

  def r2(m: Any) = {
    i(m)
    writeLine(f(m), resultsFile(Config.resultsCatsFileName))
  }

  def v(m: Any) = log(m)

  def e(m: Any) = log("ERROR: " + m)

  private def log(m: Any) = {
    super.log.error(marker, m.toString)
  }
}
