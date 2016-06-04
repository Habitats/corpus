package no.habitats.corpus.common

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.apache.spark.Logging
import org.slf4j.MarkerFactory

object Log extends Logging {
  val marker = MarkerFactory.getMarker("CORPUS")

  //  def resultsFile(name: String) = {
  //    val resultsFile = new File(s"${Config.dataPath}res/$name.txt")
  //    resultsFile.getParentFile.mkdirs
  //    if (!resultsFile.exists) {
  //      i(s"Creating results file at ${resultsFile.getAbsolutePath} ...")
  //      resultsFile.createNewFile
  //    }
  //    resultsFile
  //  }

  private def writeLine(m: String, file: File) = synchronized {
    val writer = new PrintWriter(new FileOutputStream(file, true))
    writer.println(m)
    writer.close()
  }

  private def writeLines(m: Seq[String], file: File) = synchronized {
    val writer = new PrintWriter(new FileOutputStream(file, true))
    m.foreach(writer.println)
    writer.close()
  }

  def toFile(m: String, fileName: String) = {
    val padded = if (Config.parallelism > 1) f"$fileName%-69s" else fileName
    log(s"$padded - " + m)
    saveToFile(f(m), fileName, overwrite = false)
  }

  def saveToFile(m: String, fileName: String, overwrite: Boolean = true) = {
    val resultsFile = new File(fileName)
    if (overwrite) {
      Log.i(s"${if (resultsFile.exists) "Overwriting" else "Creating "} file ${resultsFile.getAbsolutePath} ... ")
      FileUtils.deleteQuietly(resultsFile)
    }
    if (!resultsFile.exists) {
      resultsFile.getParentFile.mkdirs()
      resultsFile.createNewFile()
    }
    writeLine(m, resultsFile)
  }

  def saveToList(m: Traversable[String], fileName: String, overwrite: Boolean = true) = {
    saveToFile(m = m.map(_.toString).mkString("\n"), fileName = fileName, overwrite = overwrite)
  }

  def init() = {
    Config.cats.foreach(c => saveToFile("\n", Config.dataPath + s"spam/$c.txt", overwrite = false))
    Config.cats.foreach(c => saveToFile("Args: " + Config.getArgs.toString, Config.dataPath + s"spam/$c.txt", overwrite = false))
  }

  def f(m: Any): String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " > " + m

  def i(m: Any) = log(m)

  def v(m: Any) = log(m)

  def e(m: Any) = log("ERROR: " + m)

  private def log(m: Any) = {
    super.log.error(marker, m.toString)
  }
}
