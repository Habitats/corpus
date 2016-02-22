package no.habitats.corpus.spark

import no.habitats.corpus._
import no.habitats.corpus.models.Article
import no.habitats.corpus.npl.{Spotlight, WikiData}
import no.habitats.corpus.spark.CorpusContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.collection._

object SparkUtil {
  val cacheDir = "cache"
  var iter = 0

  def sparkTest() = {
    Log.v(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
  }

  lazy val rdd: RDD[Article] = RddFetcher.rdd(sc)

  def trainNaiveBayes() = {
    val prefs = Prefs(termFrequencyThreshold = 30, wikiDataIncludeBroad = false, wikiDataOnly = false)
    val rdd = Preprocess.computeTfIdf(RddFetcher.rdd(sc).filter(_.ann.nonEmpty).filter(_.iptc.nonEmpty))
    ML.multiLabelClassification(sc.broadcast[Prefs](prefs), rdd)
  }


  def main(args: Array[String]) = {
    Config.setArgs(args)

    Log.init()
    Log.r("Starting Corpus job ...")
    val s = System.currentTimeMillis

    Log.i(f"Loading articles ...")

    Config.job match {
      case "test" => Log.r(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
      case "preprocess" => Preprocess.preprocess(sc, sc.broadcast(Prefs()), rdd)
      case "cacheNYT" => JsonSingle.cache(Config.count)
      case "loadNYT" => Log.v(f"Loaded ${JsonSingle.load(Config.count).size} articles")
      case "printArticles" => printArticles(Config.count)
      case "computeDbAnnotations" => computeDbAnnotations()
      case "trainNaiveBayes" => trainNaiveBayes()
      case "wdToFbFromDump" => WikiData.extractFbFromWikiDump()
      case "dbpediaToWikiFromDump" => WikiData.extractWdFromDbpediaSameAsDump()
      case "combineIds" => Spotlight.combineAndCacheIds()
      case "iptcDistribution" => calculateIPTCDistribution
      case "fullCache" => fullCache()
      case "count" => Log.r(s"Counting job: ${rdd.count} articles ...")
      case _ => Log.r("No job ... Exiting!")
    }
    Log.r("Job completed in " + ((System.currentTimeMillis - s) / 1000) + " seconds")
    Thread.sleep(Long.MaxValue)
    //    sc.stop
  }

  /** Fetch json RDD and compute IPTC and annotations */
  def fullCache() = {
    RddFetcher.rdd(sc)
      .map(Corpus.toIPTC)
      .map(Corpus.toDBPediaAnnotated)
      .map(JsonSingle.toSingleJson)
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "nyt_with_all_" + DateTime.now.secondOfDay.get)
  }

  def computeDbAnnotations() = {
    Spotlight.cacheDbpedia(RddFetcher.rdd(sc), 0.5)
    Spotlight.cacheDbpedia(RddFetcher.rdd(sc), 0.75)
  }

  def printArticles(count: Int) = {
    val rddNYT = sc.parallelize(IO.walk(Config.dataPath + "/nyt/", count = count, filter = ".xml"))
      .map(Corpus.toNYT)
    Log.v("FIRST SIZE: " + rddNYT.count)
    Log.v("Article: " + Article("asd"))
    Log.v(rddNYT.collect().map(_.toString).mkString(f"SIMPLE PRINT (${rddNYT.count} articles)\n", "\n", ""))
    val rdd = rddNYT.map(Corpus.toArticle)
    Log.v(rdd.collect().map(_.toString).mkString(f"SIMPLE PRINT (${rdd.count} articles)\n", "\n", ""))
  }

  def calculateIPTCDistribution() = {
    val rdd = RddFetcher.rdd(sc)
      .flatMap(_.iptc.toSeq)
      .map(c => (c, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
    Log.v(rdd.collect.map(c => f"${c._2}%-10s - ${c._1}").mkString("IPTC CATEGORY DISTRIBUTION\n", "\n", ""))
  }

  /////////////////////
  // Utility methods //
  /////////////////////

  def wordCount(rdd: RDD[Article]): Map[String, Int] = {
    rdd.map(_.body).flatMap(_.split("\\s+"))
      .map(w => w.replaceAll("[^a-zA-Z0-9]", ""))
      .map(w => (w, 1)).reduceByKey(_ + _)
      .collect.sortBy(_._2).reverse.toMap
  }

  def memstat() = {
    //Getting the runtime reference from system
    val rt = Runtime.getRuntime
    val mb = 1024 * 1024
    Log.i(s"Used Memory: ${(rt.totalMemory - rt.freeMemory) / mb} - Free Memory: ${rt.freeMemory / mb} - Total Memory: ${rt.totalMemory / mb} - Max Memory: ${rt.maxMemory / mb}")
  }

  def debugPrint(rdd: RDD[Article], numArticles: Int = 1): Unit = {
    Log.v(s"Debug print - Articles: ${rdd.count} - Annotations: ${rdd.flatMap(_.ann).count} ...")
    rdd.take(numArticles).foreach(a => {
      Log.v(a)
      a.ann.values.foreach(Log.v)
      Log.v("")
    })
  }
}

