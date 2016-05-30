package no.habitats.corpus.common

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.apache.spark.Logging
import org.slf4j.MarkerFactory

import scala.collection.mutable

object Log extends Logging {
  val marker                       = MarkerFactory.getMarker("CORPUS")
  val headers: mutable.Set[String] = mutable.Set[String]()

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

  def toFileHeader(m: String, fileName: String, rootDir: String = Config.dataPath, overwrite: Boolean = false) = {
    if (!headers.contains(fileName + m)) toFile(m, fileName, rootDir, overwrite)
    headers.add(fileName + m)
  }
  def toFile(m: String, fileName: String, rootDir: String = Config.dataPath, overwrite: Boolean = false) = {
    val resultsFile = new File(rootDir + f"/$fileName")
    if (overwrite) {
      Log.i(s"Creating custom file at ${resultsFile.getAbsolutePath} ...")
      FileUtils.deleteQuietly(resultsFile)
    }
    if (!resultsFile.exists) {
      resultsFile.getParentFile.mkdirs()
      resultsFile.createNewFile()
    }
    writeLine(m, resultsFile)
  }

  def toListFile(m: Traversable[String], fileName: String, path: String = Config.dataPath) = {
    toFile(m.mkString("\n"), fileName, path)
  }

  def init() = {
    Config.cats.foreach(c => toFile("\n", s"res/spam/$c.txt"))
    Config.cats.foreach(c => toFile(Config.argString, s"res/spam/$c.txt"))
  }

  def f(m: Any): String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " > " + m

  def i(m: Any) = log(m)

  def v(m: Any) = log(m)

  def e(m: Any) = log("ERROR: " + m)

  private def log(m: Any) = {
    super.log.error(marker, m.toString)
  }
}
