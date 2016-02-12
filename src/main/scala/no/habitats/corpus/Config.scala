package no.habitats.corpus

import java.io.FileNotFoundException
import java.util.Properties

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

object Config {
  private var args: Arguments = Arguments()
  private var sparkConfig: String = null
  private var corpusConfig: String = null

  def setArgs(arr: Array[String]) = {
    lazy val props: Map[String, String] = arr.map(_.split("=") match { case Array(k, v) => k -> v }).toMap
    args = Arguments(
      local = props.get("local").map(_.toBoolean),
      partitions = props.get("partitions").map(_.toInt),
      rdd = props.get("rdd"),
      job = props.get("job"),
      count = props.get("count").map(_.toInt)
    )

    corpusConfig = if (local) "/corpus_local.properties" else "/corpus_cluster.properties"
    sparkConfig = if (local) "/spark_local.properties" else "/spark_cluster.properties"
    Log.v("ARGUMENTS: " + props.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t", "\n\t", ""))
    Log.v("CORPUS CONFIG: " + corpusConfig + "\n\t" + conf.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
    Log.v("SPARK CONFIG: " + sparkConfig + "\n\t" + sparkProps.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
  }

  def dataFile(s: String): BufferedSource = Try(Source.fromFile(dataPath + s)(Codec.ISO8859)) match {
    case Success(file) => Log.v("Loading data file: " + s); file
    case Failure(ex) => throw new FileNotFoundException(s"Error loading data file: $s - ERROR: ${ex.getMessage}")
  }

  def testFile(s: String): BufferedSource = Try(Source.fromFile(testPath + s)(Codec.ISO8859)) match {
    case Success(file) => Log.v("Loading test file: " + s); file
    case Failure(ex) => throw new FileNotFoundException(s"Error loading test file: $s - ERROR: ${ex.getMessage}")
  }

  lazy val sparkProps = {
    if (sparkConfig == null) {
      Log.v("NO SPARK CONFIG, USING DEFAULT. THIS SHOULD ONLY HAPPEN DURING TESTING.")
      sparkConfig = "/spark_local.properties"
    }
    val conf = new Properties
    conf.load(Source.fromInputStream(getClass.getResourceAsStream(sparkConfig))(Codec.UTF8).bufferedReader())
    conf
  }

  lazy val conf = {
    if (corpusConfig == null) {
      Log.v("NO CORPUS CONFIG, USING DEFAULT. THIS SHOULD ONLY HAPPEN DURING TESTING.")
      corpusConfig = "/corpus_local.properties"
    }
    val conf = new Properties
    conf.load(Source.fromInputStream(getClass.getResourceAsStream(corpusConfig))(Codec.UTF8).bufferedReader())
    conf
  }

  // static
  lazy val testPath: String = conf.getProperty("test_path")
  lazy val dataPath: String = conf.getProperty("data_path")
  lazy val cachePath: String = conf.getProperty("cache_path")
  lazy val broadMatch: Boolean = conf.getProperty("broad_match").toBoolean
  lazy val wikiDataOnly: Boolean = conf.getProperty("wikidata_only").toBoolean
  lazy val wikiDataIncludeBroad: Boolean = conf.getProperty("wikidata_include_broad").toBoolean
  lazy val phraseSkipThreshold: Int = conf.getProperty("term_frequency_threshold").toInt

  // Dynamic variables for file caching
  // TODO: this shouldn't be here. really.
  var resultsFileName = "results.txt"
  var resultsCatsFileName = "results_cats.txt"

  // args
  def rdd = args.rdd.getOrElse(conf.getProperty("rdd"))
  def partitions = args.partitions.getOrElse(conf.getProperty("partitions").toInt)
  def count = args.count.getOrElse(conf.getProperty("count").toInt)
  def job = args.job.getOrElse(conf.getProperty("job"))
  def local = args.local.getOrElse(true)

  case class Arguments(
                        local: Option[Boolean] = None,
                        partitions: Option[Int] = None,
                        rdd: Option[String] = None,
                        job: Option[String] = None,
                        count: Option[Int] = None
                      )
}
