package no.habitats.corpus

import java.util.Properties


object Config {

  private val conf = new Properties
  conf.load(getClass.getResourceAsStream("/corpus.properties"))

  // dynamic
  var cluster = false

  // static
  val bucketUrl: String = conf.getProperty("bucket_url")
  val testPath: String = conf.getProperty("test_path")
  val localCachePath: String = conf.getProperty("cache_path")
  var resultsFileName: String = conf.getProperty("results_name")
  var resultsCatsFileName: String = conf.getProperty("results_cats_name")
  val broadMatch: Boolean = conf.getProperty("broad_match").toBoolean
  val wikiDataOnly: Boolean = conf.getProperty("wikidata_only").toBoolean
  val wikiDataIncludeBroad: Boolean = conf.getProperty("wikidata_include_broad").toBoolean
  val wikiDataBroadOnly: Boolean = conf.getProperty("wikidata_broad_only").toBoolean
  val wikiDataBroadSalience: Double = conf.getProperty("wikidata_broad_salience").toDouble
  val salientOnly: Boolean = conf.getProperty("salient_only").toBoolean
  val salience: Double = conf.getProperty("salience").toDouble
  val phraseSkipThreshold: Int = conf.getProperty("term_frequency_threshold").toInt
  lazy val cachePath: String = if (Config.cluster) "/home/" else Config.localCachePath

  // args
  var rdd = conf.getProperty("rdd")
  var partitions = conf.getProperty("partitions").toInt
  var data = conf.getProperty("data")
  var job = conf.getProperty("job")
  val logLevel = conf.getProperty("log_level")
}
