package no.habitats.corpus

import java.io.{File, FileNotFoundException}
import java.util.Properties

import org.slf4j.LoggerFactory

import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

object Config {
  val log = LoggerFactory.getLogger(getClass)

  def dataFile(s: String): BufferedSource = Try(Source.fromFile(dataPath + s)(Codec.ISO8859)) match {
    case Success(file) => log.info("Loading data file: " + s); file
    case Failure(ex) => throw new FileNotFoundException(s"Error loading data file: $s - ERROR: ${ex.getMessage}")
  }

  def testFile(s: String): BufferedSource = Try(Source.fromFile(testPath + s)(Codec.ISO8859)) match {
    case Success(file) => log.info("Loading test file: " + s); file
    case Failure(ex) => throw new FileNotFoundException(s"Error loading test file: $s - ERROR: ${ex.getMessage}")
  }

  lazy val sparkProps = {
    val conf = new Properties
    conf.load(Source.fromInputStream(getClass.getResourceAsStream(sparkConfig))(Codec.UTF8).bufferedReader())
    conf
  }

  private lazy val conf = {
    val conf = new Properties
    conf.load(Source.fromInputStream(getClass.getResourceAsStream(corpusConfig))(Codec.UTF8).bufferedReader())
    conf
  }

  // dynamic
  var local = true
  lazy val corpusConfig = {
    val conf = if (local) "/corpus_local.properties" else "/corpus_cluster.properties"
    log.info("CORPUS CONFIG: " + conf)
    conf
  }
  lazy val sparkConfig = {
    val conf = if (local) "/spark_local.properties" else "/spark_cluster.properties"
    log.info("SPARK CONFIG: " + conf)
    conf
  }

  // static
  val testPath: String = conf.getProperty("test_path")
  val dataPath: String = conf.getProperty("data_path")
  val cachePath: String = conf.getProperty("cache_path")

  var resultsFileName: String = conf.getProperty("results_name")

  var resultsCatsFileName: String = conf.getProperty("results_cats_name")
  val broadMatch: Boolean = conf.getProperty("broad_match").toBoolean
  val wikiDataOnly: Boolean = conf.getProperty("wikidata_only").toBoolean
  val wikiDataIncludeBroad: Boolean = conf.getProperty("wikidata_include_broad").toBoolean

  val phraseSkipThreshold: Int = conf.getProperty("term_frequency_threshold").toInt

  // args
  var rdd = conf.getProperty("rdd")
  var partitions = conf.getProperty("partitions").toInt
  var data = conf.getProperty("data")
  var job = conf.getProperty("job")
  val logLevel = conf.getProperty("log_level")
}
