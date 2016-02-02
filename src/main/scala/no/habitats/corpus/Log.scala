package no.habitats.corpus

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


/**
 * Created by mail on 10.11.2015.
 */
object Log {
  def resultsFile(name: String) = {
    new File(Config.cachePath + "res/").mkdirs()
    val resultsFile = new File(Config.cachePath + "res/" + Config.data + "_" + name + ".txt")
    if (!resultsFile.exists) {
      Log.i(s"Creating results file at ${resultsFile.getAbsolutePath} ...")
      resultsFile.createNewFile
    }
    resultsFile
  }

  def writeLine(m: String, file: File) = {
    val writer = new PrintWriter(new FileOutputStream(file, true))
    writer.println(m)
    writer.close()
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

  def toFile(m: Any, name: String) = {
    val resultsFile = new File("stats/" + name + ".txt")
    if (!resultsFile.exists) {
      Log.i(s"Creating custom file at ${resultsFile.getAbsolutePath} ...")
      resultsFile.createNewFile
    }
    writeLine(m.toString, resultsFile)
  }

  def v(m: Any) = if (Config.logLevel == "all") log(m)

  def e(m: Any) = if (Config.logLevel == "all") log("ERROR: " + m)

  private def log(m: Any) = println(f(m))
}
