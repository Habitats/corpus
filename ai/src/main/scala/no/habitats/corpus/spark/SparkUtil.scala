package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.FreebaseW2V
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.TSNE
import org.apache.spark.rdd.RDD

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
      case "testSpark" => Log.r(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
      case "printArticles" => printArticles(Config.count)
      case "misc" =>
//        Log.v(Config.cats.mkString(", "))
        Cacher.split(Fetcher.annotatedRdd, 10)

      // Generate datasets
      case "cacheNYT" => JsonSingle.cacheRawNYTtoJson()
      case "computeDbAnnotations" =>
        val ids: Set[String] = (Fetcher.subTrainOrdered ++ Fetcher.subTestOrdered ++ Fetcher.subValidationOrdered).map(_.id).collect.toSet
        Cacher.computeAndCacheDBPediaAnnotationsToJson(Fetcher.rdd.filter(a => ids.contains(a.id)).sortBy(_.id.toInt))
      case "computeDbAnnotationsConfidence" => Cacher.annotateAndCacheArticlesConfidence()

      case "wdToFbFromDump" => WikiData.extractFreebaseFromWikiDump()
      case "dbpediaToWdFromDump" => WikiData.extractWikiIDFromDbpediaDump()
      case "combineIds" => Spotlight.combineAndCacheIds()
      case "fbw2v" => FreebaseW2V.cacheWordVectors(Fetcher.miniMini25, 0.25)
      case "fbw2vIds" => FreebaseW2V.cacheFbIds()
      case "cacheW2V" => FreebaseW2V.cacheAll()
      case "cacheDocumentVectors" =>
        W2VLoader.cacheDocumentVectors(Fetcher.annotatedRddMini)
        W2VLoader.cacheDocumentVectors(Fetcher.miniMini25)

      case "cacheAnnotated" => Cacher.annotateAndCacheArticles()
      case "cacheMiniCorpus" => Cacher.cacheMiniCorpus()
      case "cacheAnnotatedWithTypes" => Cacher.annotateAndCacheArticlesWithTypes()
      case "splitAndCache" => Cacher.splitAndCache() // REQUIREMENT FOR TRAINING
      case "cacheBalanced" => Cacher.cacheBalanced()
      case "cacheMinimal" => Cacher.cacheMinimalArticles(Fetcher.annotatedRdd, "nyt_corpus_annotated")
      case "cacheSuperSampled" => Cacher.cacheSuperSampled(Some(100000))
      case "cacheSubSampled" =>
        Cacher.cacheSubSampledOrdered()
        Cacher.cacheSubSampledShuffled()
      case "cacheAndSplitLength" => Cacher.cacheAndSplitLength()
      case "cacheAndSplitTime" => Cacher.cacheAndSplitTime()
      case "cache" =>
        Seq(25, 50, 75, 100).foreach(s => Cacher.splitOrdered(Fetcher.by("confidence/nyt_mini_train_annotated_" + s + ".txt"), s.toString))
      //        Cacher.cacheAndSplitLength()
      //        Cacher.cacheAndSplitTime()
      //        Cacher.cacheSubSampledOrdered()
      //        Cacher.cacheSubSampledShuffled()

      // Display stats
      case "iptcDistribution" => calculateIPTCDistribution()
      case "tnesDocumentVectors" => tnesDocumentVectors()
      case "tnesWordVectors" => tnesWordVectors()
      case "stats" =>
        //        CorpusStats(Fetcher.annotatedTrainOrdered, "filtered").termFrequencyAnalysis()
        //        CorpusStats(Fetcher.by("time/nyt_time_train.txt"), "time").termFrequencyAnalysis()
        CorpusStats(Fetcher.annotatedTrainOrdered, "filtered").lengthCorrelation()
      //        Corpus.preloadAnnotations()
      //        stats(Fetcher.rdd.map(Corpus.toDBPediaAnnotated), "original")
      //        timeStats()
      //        lengthStats()

      // Modelling
      case "trainRNNBalanced" => Trainer.trainRNNBalanced()
      case "trainFFNOrdered" => Trainer.trainFFNOrdered()
      case "trainFFNBoWOrdered" => Trainer.trainFFNBoWOrdered()
      case "trainFFNShuffled" => Trainer.trainFFNShuffled()
      case "trainFFNBalanced" => Trainer.trainFFNBalanced()
      case "trainFFNConfidence" => Trainer.trainFFNConfidence()

      case "trainFFNW2VSubsampled" => Trainer.trainFFNW2VSubsampled()
      case "trainFFNBoWSubsampled" => Trainer.trainFFNBoWSubsampled()
      case "trainRNNSubsampled" => Trainer.trainRNNSubsampled()

      case "train" =>
        // DONE
        // Trainer.trainNaiveBayesW2VSubsampled()
        // Trainer.trainNaiveBayesBoWSubsampled()
        // Trainer.trainFFNW2VSubsampled()
        // Trainer.trainFFNBoWSubsampled()

        // Trainer.trainFFNOrdered()

        // TODO

//        Trainer.trainRNNSubsampled()

        Trainer.trainFFNBoWTime()
        Trainer.trainFFNW2VTime()

//        Trainer.trainFFNConfidence()
//        Trainer.trainFFNOrderedTypes(true)
//
//        Trainer.trainFFNBoWOrdered()
//        Trainer.trainFFNOrderedTypes(false)

      // Testing
      case "testModels" => Tester.testModels()
      case "testFFNBoW" => Tester.testFFNBow()
      case "testLengths" => Tester.testLengths()
      case "testTimeDecay" => Tester.testTimeDecay()
      case "testConfidence" => Tester.testConfidence()
      case "test" =>
        //        Tester.testModels()
        //        // Ex 1
        //        Tester.testEmbeddedVeBoW()
        //        // Ex 2
        //        Tester.testTypesInclusion()
        //        // Ex 3
        //        Tester.testLengths()
        // Ex 4
//        Tester.testShuffledVsOrdered()
              Tester.testTimeDecay()
      // Ex 5
      //        Tester.testConfidence()

      case _ => Log.r("No job ... Exiting!")
    }
    Log.r(s"Job completed in${prettyTime(System.currentTimeMillis - s)}")
    //    Thread.sleep(Long.MaxValue)
    sc.stop
    System.exit(0)
  }

  def misc() = {
    val rdd = Fetcher.subTrainOrdered.filter(_.body.length > 20e3)
    rdd.foreach(Log.v)
  }

  def tnesDocumentVectors() = {
    TSNE.create(Fetcher.subTrainOrdered, useDocumentVectors = true)
  }

  def tnesWordVectors() = {
    TSNE.create(Fetcher.subTrainOrdered, useDocumentVectors = false)
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

  def lengthStats() = borderStats("test_length", _.wc)
  def timeStats() = borderStats("test_time", _.id.toInt)

  def borderStats(filter: String, criterion: Article => Double) = {
    val groups: Array[String] = new File(Config.dataPath + "nyt").listFiles.map(_.getName).filter(_.contains(filter))
    for (i <- groups) {
      val r0 = Fetcher.fetch(s"nyt/$i").map(criterion)
      r0.cache
      val min = r0.min.toInt
      val max = r0.max.toInt
      Log.v(f"Border stats for $filter - count: ${r0.count.toInt}%7d - min: $min%7d - max: $max%7d")
    }
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
    s"$days$hours$minutes$seconds ($ms ms)"
  }
}

