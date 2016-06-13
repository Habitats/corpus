package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.FreebaseW2V
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.TSNE
import org.apache.spark.rdd.RDD
import org.nd4j.linalg.factory.Nd4j

import scala.collection.Map
import scala.util.Try

object SparkUtil {
  val cacheDir = "cache"
  var iter     = 0

  def sparkTest() = {
    Log.v(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
  }

  def main(args: Array[String]) = {
    Nd4j.create(1)
    Try {
      Config.setArgs(args)
      Log.init()
      Log.v(s"Starting Corpus job: ${args.mkString(", ")}")
      val s = System.currentTimeMillis

      Log.i(f"Loading articles ...")

      Config.job match {
        // Misc
        case "testSpark" => Log.v(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
        case "printArticles" => printArticles(Config.count)
        case "misc" =>
          //          Tester.testTimeDecay()
          Tester.testLengths()

        // Generate datasets
        case "cacheNYT" => JsonSingle.cacheRawNYTtoJson()
        case "computeDbAnnotations" => Cacher.computeAndCacheDBPediaAnnotationsToJson(Fetcher.rdd.sortBy(_.id.toInt))
        case "computeDbAnnotationsConfidence" => Cacher.annotateAndCacheArticles()

        case "wdToFbFromDump" => WikiData.extractFreebaseFromWikiDump()
        case "dbpediaToWdFromDump" => WikiData.extractWikiIDFromDbpediaDump()
        case "combineIds" => Spotlight.combineAndCacheIds()
        case "fbw2vIds" => FreebaseW2V.cacheFbIds()
        case "cacheW2V" => FreebaseW2V.cacheAll()
        case "cacheWordVectors" => for {
          c <- Seq(0.25, 0.75, 1.0)
          t <- Seq(true, false)
        } yield Fetcher.ordered(confidence = c, types = t)._1

        case "cacheAnnotated" => Cacher.annotateAndCacheArticles(confidence = 0.25)
        case "cacheMiniCorpus" => Cacher.cacheMiniCorpus()
        case "cacheAnnotatedWithTypes" => Cacher.annotateAndCacheArticlesWithTypes()
        case "splitAndCache" => Cacher.splitAndCache() // REQUIREMENT FOR TRAINING
        case "cacheSuperBalanced" => Cacher.cacheSupersampledBalanced()
        case "cacheMinimal" => Cacher.cacheMinimalArticles(Fetcher.annotatedRdd, "nyt_corpus_annotated")
        case "cacheSuperSampled" => Cacher.cacheSuperSampled(Some(100000))
        case "cacheSubSampled" =>
          Cacher.cacheSubSampledOrdered()
          Cacher.cacheSubSampledShuffled()
        case "cacheAndSplitLength" => Cacher.cacheAndSplitLength()
        case "cacheAndSplitTime" => Cacher.cacheAndSplitTime()
        case "cacheAndSplitCorpus" => Cacher.splitCustom()
        case "cache" =>
          Seq(25, 50, 75, 100).foreach(s => Cacher.splitOrdered(Fetcher.by("confidence/nyt_mini_train_annotated_" + s + ".txt"), s.toString))

        // Display stats
        case "iptcDistribution" => calculateIPTCDistribution()
        case "tnesDocumentVectors" => tnesDocumentVectors()
        case "tnesWordVectors" => tnesWordVectors()
        case "stats" =>
          //          CorpusStats(Fetcher.annotatedTrainOrdered, "filtered").compute()
          //          CorpusStats(Fetcher.annotatedTrainOrderedTypes, "filtered-types").compute()
          //          CorpusStats(Fetcher.annotatedTestOrdered, "filtered-test").compute()
          //          CorpusStats(Fetcher.annotatedTestOrderedTypes, "filtered-types-test").compute()
          //          CorpusStats(Fetcher.annotatedValidationOrdered, "filtered-valid").compute()
          //          CorpusStats(Fetcher.annotatedValidationOrderedTypes, "filtered-types-valid").compute()

          val train = Fetcher.by("time/nyt_time_10_train.txt")
          val tfidf = TFIDF(train, 0, "time/")
          new File(Config.dataPath + "nyt/time").listFiles().filter(_.isFile).map(_.getName).filter(_.contains("test")).map(f => (Fetcher.by("time/" + f), f)).map(a => (a._1.map(_.filterAnnotation(an => tfidf.contains(an.id))), a._2)).foreach(rdd => CorpusStats(rdd._1, "time/" + rdd._2).compute())
        //          CorpusStats(Fetcher.by("types/nyt_train_ordered_types.txt"), "types").annotationStatistics()
//                  CorpusStats(Fetcher.by("types/nyt_train_ordered_types.txt"), "types").compute()

        // Modelling
        case "trainRNNW2V" => Trainer.trainRNNW2V()
        case "trainFFNW2V" => Trainer.trainFFNW2V()
        case "trainFFNBoW" => Trainer.trainFFNBoW()
        case "trainNaiveW2V" => Trainer.trainNaiveW2V()
        case "trainNaiveBoW" => Trainer.trainNaiveBoW()
        case "trainNaiveTraditional" => Trainer.trainNaiveTraditional()

        case "trainBaseline" => Trainer.baseline()
        case "trainSuper" => Trainer.supersampled()
        case "trainTypes" => Trainer.types()
        case "trainTime" => Trainer.time()
        case "trainConfidence" => Trainer.confidence()

        // Testing
        case "testModels" => Tester.testModels()
        case "testLengths" => Tester.testLengths()
        case "testTimeDecay" => Tester.testTimeDecay()
        case "testConfidence" => Tester.testConfidence()
        case "testW2VvsBoW" => Tester.testEmbeddedVsBoW()
        case "testPretrained" => Tester.testPretrained()
        case "testNaive" => Tester.testNaive()
        case "testSub" =>
        //          Tester.sub
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
        case _ => Log.v("No job ... Exiting!")
      }
      Log.v(s"Job completed in${prettyTime(System.currentTimeMillis - s)}")
    }.failed.map(f => {Log.e("Crash: " + f); f.printStackTrace()})
    // If on cluster, pause to avoid termination of the screen/terminal
    if (!Config.local)
      Thread.sleep(Long.MaxValue)
    else {
      sc.stop
      System.exit(0)
    }
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

  def prettyTime(ms: Long, short: Boolean = false): String = {
    var x = ms / 1000
    val seconds = x % 60 match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e" + (if (short) "s" else " second")
      case e => f" $e" + (if (short) "s" else " seconds")
    }
    x /= 60
    val minutes = x % 60 match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e" + (if (short) "m" else " minute")
      case e => f" $e" + (if (short) "m" else " minutes")
    }
    x /= 60
    val hours = x % 24 match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e" + (if (short) "h" else " hour")
      case e => f" $e" + (if (short) "h" else " hours")
    }
    x /= 24
    val days = x match {
      case e if e == 0 => ""
      case e if e == 1 => f" $e" + (if (short) "d" else " day")
      case e => f" $e" + (if (short) "d" else " days")
    }
    s"$days$hours$minutes$seconds" + (if (short) "" else s" ($ms ms)")
  }
}
