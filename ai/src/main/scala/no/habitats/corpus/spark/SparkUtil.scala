package no.habitats.corpus.spark

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.dl4j.FreebaseW2V
import no.habitats.corpus.models.{Annotation, Article}
import no.habitats.corpus.npl.{IPTC, Spotlight, WikiData}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD

import scala.collection._

object SparkUtil {
  val cacheDir = "cache"
  var iter     = 0

  def sparkTest() = {
    Log.v(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
  }

  lazy val rdd: RDD[Article] = RddFetcher.annotatedRdd

  def trainNaiveBayes() = {
    val prefs = sc.broadcast[Prefs](Prefs(termFrequencyThreshold = 5, wikiDataIncludeBroad = false, wikiDataOnly = false))
    val preprocessed = Preprocess.preprocess(prefs, rdd)
    ML.multiLabelClassification(prefs, preprocessed)
  }

  def main(args: Array[String]) = {
    Config.setArgs(args)

    Log.init()
    Log.r(s"Starting Corpus job: ${args.mkString(", ")}")
    val s = System.currentTimeMillis

    Log.i(f"Loading articles ...")

    Config.job match {
      // Misc
      case "test" => Log.r(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
      case "printArticles" => printArticles(Config.count)
      case "count" => Log.r(s"Counting job: ${rdd.count} articles ...")
      case "preprocess" => Preprocess.preprocess(sc.broadcast(Prefs()), rdd)

      // Generate datasets
      case "cacheNYT" => JsonSingle.cacheRawNYTtoJson()
      case "computeDbAnnotations" => computeAndCacheDBPediaAnnotationsToJson()
      case "wdToFbFromDump" => WikiData.extractFreebaseFromWikiDump()
      case "dbpediaToWdFromDump" => WikiData.extractWikiIDFromDbpediaDump()
      case "combineIds" => Spotlight.combineAndCacheIds()
      case "cacheAnnotated" => annotateAndCacheArticles()
      case "cacheAnnotatedW2V" => annotateAndCacheArticlesWithW2V()
      case "fullCacheSep" => annotateAndCacheSeparateArticles()
      case "cacheBalanced" => cacheBalanced()
      case "cacheMinimal" => cacheMinimalArticles()
      case "fbw2v" => FreebaseW2V.cacheWordVectors()
      case "fbw2vids" => FreebaseW2V.cacheWordVectorIds()
      case "cacheTest" => cacheTest()

      // Display stats
      case "iptcDistribution" => calculateIPTCDistribution()

      // Modelling
      case "trainNaiveBayes" => trainNaiveBayes()
      case "trainRNNML" => FreebaseW2V.trainMultiLabelRNN()
      case "trainRNN" => IPTC.topCategories.foreach(c => NeuralModelLoader.save(FreebaseW2V.trainMultiLabelRNN(Some(c)), c, Config.count))
      case "trainRNNBalanced" => trainRNNBalanced()
      case "trainRNNSingle" => NeuralModelLoader.save(FreebaseW2V.trainMultiLabelRNN(Some(Config.category)), Config.category, Config.count) // category=?
      case "trainSparkRNN" => FreebaseW2V.trainSparkMultiLabelRNN()
      case "loadRNN" => NeuralModelLoader.load(Config.category, Config.count)
      case "testModels" => FreebaseW2V.testAllModels()

      case _ => Log.r("No job ... Exiting!")
    }
    Log.r(s"Job completed in${prettyTime(System.currentTimeMillis - s)}")
    //    Thread.sleep(Long.MaxValue)
    //    sc.stop
  }

  /** Fetch json RDD and compute IPTC and annotations */
  def annotateAndCacheArticles() = {
    val rdd = RddFetcher.rdd
      .map(Corpus.toIPTC)
      .map(Corpus.toDBPediaAnnotated)
      .map(JsonSingle.toSingleJson)
    saveAsText(rdd, "nyt_with_all")
  }

  def trainRNNBalanced() = {
    val done = Set(
      "arts, culture and entertainment",
      "conflicts, war and peace",
      "crime, law and justice",
      "disaster, accident and emergency incident",
      "economy, business and finance",
      "education",
      "environment",
      "health",
      "human interest",
      "labour"
    )
    IPTC.topCategories.filter(c => !done.contains(c)).foreach(c => {
      val rdd = RddFetcher.balanced(IPTC.trim(c))
      val split = rdd.randomSplit(Array(0.8, 0.2), Config.seed)
      NeuralModelLoader.save(FreebaseW2V.trainMultiLabelRNN(Some(c), split(0), split(1)), c, Config.count)
    })
  }

  def cacheTest() = {
    val test = RddFetcher.annotatedRdd.randomSplit(Array(0.99, 0.01), Config.seed)(1)
      .filter(_.iptc.nonEmpty)
      .map(a => a.copy(ann = a.ann.filter(an => an._2.fb != Annotation.NONE && W2VLoader.contains(an._2.fb))))
      .filter(_.ann.nonEmpty)
      .map(JsonSingle.toSingleJson)
    saveAsText(test, "test_annotated")
  }

  def cacheMinimalArticles() = {
    val minimal = RddFetcher.annotatedW2VRdd
      .filter(_.iptc.nonEmpty)
      .map(a => f"${a.id} ${a.iptc.map(IPTC.trim).mkString(",")} ${a.ann.map(_._2.fb).mkString(",")}")
    saveAsText(minimal, "minimal")
  }

  def cacheBalanced() = {
    val all = RddFetcher.annotatedW2VRdd
    IPTC.topCategories
      //    Set("weather")
      .foreach(c => {
      val balanced = RddFetcher.createBalanced(IPTC.trim(c), all).filter(_.iptc.nonEmpty)
      SparkUtil.saveAsText(balanced.map(JsonSingle.toSingleJson), c + "_balanced")
    })
  }

  def annotateAndCacheArticlesWithW2V() = {
    val rdd = RddFetcher.annotatedRdd
      .map(a => a.copy(ann = a.ann.filter(an => an._2.fb != Annotation.NONE && W2VLoader.contains(an._2.fb))))
      .filter(_.ann.size >= Config.phraseSkipThreshold)
      .map(JsonSingle.toSingleJson)
    saveAsText(rdd, "nyt_with_all_w2v_" + Config.phraseSkipThreshold)
  }

  def annotateAndCacheSeparateArticles() = {
    val rdd = RddFetcher.annotatedW2VRdd
    rdd.cache()
    IPTC.topCategories.foreach(c => {
      val filtered = rdd
        .filter(_.iptc.contains(c))
        .map(JsonSingle.toSingleJson)
      saveAsText(filtered, c)
    })
  }

  def computeAndCacheDBPediaAnnotationsToJson() = {
    Spotlight.cacheDbpedia(RddFetcher.rdd, 0.5)
    Spotlight.cacheDbpedia(RddFetcher.rdd, 0.75)
  }

  def saveAsText(rdd: RDD[String], name: String) = {
    val path = Config.cachePath + s"${name.replaceAll("[,\\s+]+", "_")}"
    FileUtils.deleteDirectory(new File(path))
    rdd.coalesce(1, shuffle = true).saveAsTextFile(path)
    val file = new File(path + ".json")
    Files.move(new File(path + "/part-00000").toPath, file.toPath, StandardCopyOption.REPLACE_EXISTING)
    FileUtils.deleteDirectory(new File(path))
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
    val rdd = RddFetcher.rdd
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
      Log.v("")
      a.ann.values.foreach(Log.v)
    })
  }

  def prettyTime(ms: Long): String = {
    var x = ms / 1000
    val seconds = x % 60 match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e second"
      case e if e > 0 => f" $e seconds"
    }
    x /= 60
    val minutes = x % 60 match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e minute"
      case e if e > 0 => f" $e minutes"
    }
    x /= 60
    val hours = x % 24 match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e hour"
      case e if e > 0 => f" $e hours"
    }
    x /= 24
    val days = x match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e day"
      case e if e > 0 => f" $e days"
    }
    f"$days$hours$minutes$seconds ($ms ms)"
  }
}

