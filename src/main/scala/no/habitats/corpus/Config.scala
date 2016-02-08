package no.habitats.corpus

import java.util.Properties

import scala.io.{BufferedSource, Codec, Source}

object Config {
  def dataFile(s: String): BufferedSource = Source.fromFile(dataRoot + s)(Codec.ISO8859)

  private val conf = new Properties
  conf.load(Source.fromFile("corpus.properties")(Codec.UTF8).bufferedReader())

  // dynamic
  var standalone = true

  // static
  val bucketUrl           : String  = conf.getProperty("bucket_url")
  val testPath            : String  = conf.getProperty("test_path")
  val localCachePath      : String  = conf.getProperty("cache_path")
  var resultsFileName     : String  = conf.getProperty("results_name")
  var resultsCatsFileName : String  = conf.getProperty("results_cats_name")
  val dataRoot            : String  = conf.getProperty("data_root")
  val broadMatch          : Boolean = conf.getProperty("broad_match").toBoolean
  val wikiDataOnly        : Boolean = conf.getProperty("wikidata_only").toBoolean
  val wikiDataIncludeBroad: Boolean = conf.getProperty("wikidata_include_broad").toBoolean
  val phraseSkipThreshold : Int     = conf.getProperty("term_frequency_threshold").toInt

  lazy val cachePath: String = if (Config.standalone) Config.localCachePath else "/home/"

  // args
  var rdd        = conf.getProperty("rdd")
  var partitions = conf.getProperty("partitions").toInt
  var data       = conf.getProperty("data")
  var job        = conf.getProperty("job")
  val logLevel   = conf.getProperty("log_level")
}
