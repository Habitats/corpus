package no.habitats.corpus.spark

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.{FreebaseW2V, TSNE}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import scala.collection.Map

object SparkUtil {
  val cacheDir = "cache"
  var iter     = 0

  def sparkTest() = {
    Log.v(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
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

      // Generate datasets
      case "cacheNYT" => JsonSingle.cacheRawNYTtoJson()
      case "computeDbAnnotations" => computeAndCacheDBPediaAnnotationsToJson()

      case "wdToFbFromDump" => WikiData.extractFreebaseFromWikiDump()
      case "dbpediaToWdFromDump" => WikiData.extractWikiIDFromDbpediaDump()
      case "combineIds" => Spotlight.combineAndCacheIds()
      case "fbw2v" => FreebaseW2V.cacheWordVectors()
      case "fbw2vids" => FreebaseW2V.cacheWordVectorIds()

      case "cacheAnnotated" => Cacher.annotateAndCacheArticles()
      case "splitAndCache" => Cacher.splitAndCache() // REQUIREMENT FOR TRAINING
      case "cacheBalanced" => Cacher.cacheBalanced()
      case "cacheMinimal" => Cacher.cacheMinimalArticles()
      case "cacheSuperSampled" => Cacher.cacheSuperSampled(Some(100000))
      case "cacheSubSampled" => Cacher.cacheSubSampled()
      case "cacheAndSplitLength" => Cacher.cacheAndSplitLength()

      // Display stats
      case "iptcDistribution" => calculateIPTCDistribution()
      case "tnesDocumentVectors" => tnesDocumentVectors()
      case "tnesWordVectors" => tnesWordVectors()
      case "stats" =>
        //        stats(RddFetcher.subTrainW2V)
        stats(Fetcher.annotatedRdd)

      // Modelling
      case "trainNaiveBayesBoW" => Trainer.trainNaiveBayes(bow = true)
      case "trainNaiveBayesW2V" => Trainer.trainNaiveBayes(bow = false)
      case "trainRNNSubSampled" => Trainer.trainRNNSubSampled()
      case "trainFFNSubSampled" => Trainer.trainFFNSubSampled()
      case "trainFFNSubSampledBoW" => Trainer.trainFFNSubSampledBoW()
      case "trainRNNBalanced" => Trainer.trainRNNBalanced()
      case "trainFFNBalanced" => Trainer.trainFFNBalanced()
      case "trainRNNSpark" => Trainer.trainRNNSpark()
      case "trainFFNSpark" => Trainer.trainFFNSpark()

      case "testModels" => Tester.testModels()

      case _ => Log.r("No job ... Exiting!")
    }
    Log.r(s"Job completed in${prettyTime(System.currentTimeMillis - s)}")
    //    Thread.sleep(Long.MaxValue)
    //    sc.stop
  }

  def tnesDocumentVectors() = {
    TSNE.create(Fetcher.subTrainW2V, useDocumentVectors = true)
  }

  def tnesWordVectors() = {
    TSNE.create(Fetcher.subTrainW2V, useDocumentVectors = false)
  }

  def computeAndCacheDBPediaAnnotationsToJson() = {
    Spotlight.cacheDbpedia(Fetcher.rdd, 0.5)
    Spotlight.cacheDbpedia(Fetcher.rdd, 0.75)
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
    val rdd = Fetcher.rdd
      .flatMap(_.iptc.toSeq)
      .map(c => (c, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
    Log.v(rdd.collect.map(c => f"${c._2}%-10s - ${c._1}").mkString("IPTC CATEGORY DISTRIBUTION\n", "\n", ""))
  }

  def stats(rdd: RDD[Article]) = {
    rdd.cache()
    val statsFile = "stats/general_stats.txt"

    // Annotations per IPTC
    val annCounts = rdd.flatMap(a => a.iptc.map(c => (c, a.ann.size))).reduceByKey(_ + _).collectAsMap
    Log.toFile(annCounts.toSeq.sortBy(_._1).map(c => f"${c._1}%41s ${c._2}%10d").mkString("Annotations per ITPC:\n", "\n", "\n"), statsFile)

    // Articles per IPTC (category distribution)
    val artByAnn = rdd.flatMap(a => a.iptc.map(c => (c, 1))).reduceByKey(_ + _).collectAsMap
    Log.toFile(artByAnn.toSeq.sortBy(_._1).map(c => f"${c._1}%41s ${c._2}%10d").mkString("Articles per IPTC:\n", "\n", "\n"), statsFile)

    // Average ANN per IPTC
    val iptc = rdd.flatMap(_.iptc).distinct.collect.sorted
    val avgAnnIptc = iptc.map(c => (c, annCounts(c).toDouble / artByAnn(c))).toMap
    Log.toFile(avgAnnIptc.toSeq.sortBy(_._1).map(c => f"${c._1}%41s ${c._2}%10.0f").mkString("Average number of annoations per IPTC:\n", "\n", "\n"), statsFile)

    // General stats
    val numAnnotations = rdd.flatMap(_.ann.values.toList)
    val numArticles = rdd.count
    Log.toFile(f"Articles:             ${numArticles}%10d", statsFile)
    Log.toFile(f"Total annotations:    ${numAnnotations.count}%10d", statsFile)
    Log.toFile(f"Distinct annotations: ${numAnnotations.map(_.id).distinct.count}%10d", statsFile)

    def statsToPretty(stats: StatCounter, name: String): String = f"${name}%25s - Max: ${stats.max.toInt}%10d - Min: ${stats.min.toInt}%3d - Std: ${stats.stdev}%7.2f - Mean: ${stats.mean}%7.2f - Variance: ${stats.variance}%15.2f"
    def pairs(rdd: RDD[_]): Array[String] = rdd.map(_.toString).map(i => (i, 1)).reduceByKey(_ + _).map { case (k, v) => f"$k%10s$v%10d" }.collect()

    val annotationsIptc: RDD[Int] = rdd.map(_.ann.size)
    Log.toFile(statsToPretty(annotationsIptc.stats(), "Annotations"), statsFile)
    Log.toFile(pairs(annotationsIptc), "stats/annotations_iptc.txt")

    val articlesIptc: RDD[Int] = rdd.map(_.iptc.size)
    Log.toFile(statsToPretty(articlesIptc.stats(), "IPTC"), statsFile)
    Log.toFile(pairs(articlesIptc), "stats/articles_iptc.txt")

    val articleLength: RDD[Int] = rdd.filter(_.body != null).map(_.body.length)
    Log.toFile(statsToPretty(articleLength.stats(), "Article length"), statsFile)
    Log.toFile(pairs(articleLength), "stats/article_length.txt")

    val mentionAnnotation: RDD[Int] = rdd.flatMap(_.ann.values.map(_.id)).map(id => (id, 1)).reduceByKey(_ + _).values
    Log.toFile(statsToPretty(mentionAnnotation.stats(), "Mentions per annotation"), statsFile)
    Log.toFile(pairs(mentionAnnotation), "stats/mention_annotation.txt")
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

