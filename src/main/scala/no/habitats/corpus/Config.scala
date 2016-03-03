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
      iptcFilter = props.get("iptcFilter").map(_.split(",").toSet),
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
      corpusConfig = if (System.getProperty("os.name").startsWith("Windows")) "/corpus_local.properties" else "/corpus_cluster.properties"
    }
    val conf = new Properties
    conf.load(Source.fromInputStream(getClass.getResourceAsStream(corpusConfig))(Codec.UTF8).bufferedReader())
    conf
  }

  // static
  val testPath: String = conf.getProperty("test_path").replace("~", System.getProperty("user.home"))
  val dataPath: String = conf.getProperty("data_path").replace("~", System.getProperty("user.home"))
  val cachePath: String = conf.getProperty("cache_path").replace("~", System.getProperty("user.home"))
  val dbpedia: String = conf.getProperty("dbpedia")
  val broadMatch: Boolean = conf.getProperty("broad_match").toBoolean
  val wikiDataOnly: Boolean = conf.getProperty("wikidata_only").toBoolean
  val wikiDataIncludeBroad: Boolean = conf.getProperty("wikidata_include_broad").toBoolean
  val phraseSkipThreshold: Int = conf.getProperty("term_frequency_threshold").toInt
  val iptcFilter: Set[String] = Some(conf.getProperty("iptc_filter")).map(e => if (e.length > 0) e.split(",").toSet else Set[String]()).get

  // Dynamic variables for file caching
  // TODO: this shouldn't be here. really.
  var resultsFileName = "results.txt"
  var resultsCatsFileName = "results_cats.txt"

  // args
  def rdd = args.rdd.getOrElse(conf.getProperty("rdd"))
  def partitions = args.partitions.getOrElse(conf.getProperty("partitions").toInt)
  def count: Int = args.count.getOrElse(conf.getProperty("count").toInt) match {
    case i => if (i == -1) Integer.MAX_VALUE else i
  }
  def job = args.job.getOrElse(conf.getProperty("job"))
  def local = args.local.getOrElse(true)

  case class Arguments(
                        local: Option[Boolean] = None,
                        partitions: Option[Int] = None,
                        rdd: Option[String] = None,
                        job: Option[String] = None,
                        count: Option[Int] = None,
                        iptcFilter: Option[Set[String]] = None
                      )
}
