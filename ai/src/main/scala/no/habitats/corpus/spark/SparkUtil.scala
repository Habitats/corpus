package no.habitats.corpus.spark

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.dl4j.{FreebaseW2V, NeuralPrefs}
import no.habitats.corpus.models.{Annotation, Article}
import no.habitats.corpus.npl.{IPTC, Spotlight, WikiData}
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.collection._
import scala.util.Try

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

  def cacheSubSampled() = {
    val rdds = Map(
      //      "train" -> RddFetcher.annotatedTrainW2V,
      "test" -> RddFetcher.annotatedTestW2V,
      "validation" -> RddFetcher.annotatedValidationW2V
    )
    rdds.foreach { case (k, v) => RddFetcher.cacheSubSampled(v, k) }
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
      case "splitAndCacheAnnotatedW2V" => splitAndCacheArticlesWithW2V()
      case "cacheBalanced" => cacheBalanced()
      case "cacheMinimal" => cacheMinimalArticles()
      case "cacheSuperSampled" => RddFetcher.cacheSuperSampled(Some(100000))
      case "cacheSubSampled" => cacheSubSampled()
      case "fbw2v" => FreebaseW2V.cacheWordVectors()
      case "fbw2vids" => FreebaseW2V.cacheWordVectorIds()

      // Display stats
      case "iptcDistribution" => calculateIPTCDistribution()

      // Modelling
      case "trainNaiveBayes" => trainNaiveBayes()
      case "trainRNN" => trainRNN()
      case "trainRNNBalanced" => trainRNNBalanced()
      case "trainRNNBalancedSingle" => trainRNNBalancedSingle()
      case "trainFFNBalancedSingle" => trainFFNBalancedSingle()
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

  def trainRNN() = {
    for {
      learningRate <- Seq(0.5, 0.05, 0.005, 0.0005, 0.00005)
      hiddeNodes <- Seq(333)
      category <- Try(Seq(Config.category)).getOrElse(IPTC.topCategories)
      miniBatchSize <- Seq(50)
      epochs <- Seq(5)
      (train, test) = {
        val train = RddFetcher.balanced(IPTC.trim(category) + "_train", true)
        val validation = RddFetcher.balanced(IPTC.trim(category) + "_validation", false)
        //        val split: Array[RDD[Article]] = RddFetcher.annotatedW2VRdd.randomSplit(Array(0.8, 0.2), seed = Config.seed)
        (train, validation)
      }
    } yield {
      val neuralPrefs = NeuralPrefs(learningRate, hiddeNodes, train, test, miniBatchSize, epochs)
      val n: MultiLayerNetwork = FreebaseW2V.trainBinaryRNN(category, neuralPrefs = neuralPrefs)
      //      NeuralModelLoader.save(n, category, Config.count)
    }
  }

  def trainRNNBalanced() = {
    val done = Set[String](
      //      "arts, culture and entertainment",
      //      "conflicts, war and peace",
      //      "crime, law and justice",
      //      "disaster, accident and emergency incident",
      //      "economy, business and finance",
      //      "education",
      //      "environment",
      //      "health",
      //      "human interest",
      //      "labour"
    )
    IPTC.topCategories.filter(c => !done.contains(c)).foreach(trainRNNBalancedSingle)
  }

  def trainRNNBalancedSingle(c: String = Config.category) = trainBalancedSingle(c, FreebaseW2V.trainBinaryRNN)

  def trainFFNBalancedSingle(c: String = Config.category) = trainBalancedSingle(c, FreebaseW2V.trainBinaryFFN)

  def trainBalancedSingle(c: String = Config.category, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork) = {
    //    val train = RddFetcher.balanced(IPTC.trim(c) + "_train", true)
    //    val validation = RddFetcher.balanced(IPTC.trim(c) + "_validation", false)
    val train = RddFetcher.subTrainW2V
    val validation = RddFetcher.subValidationW2V
    for {
      hiddenNodes <- Seq(10)
      //      hiddenNodes <- Seq(1, 5, 10, 20, 50, 100, 200)
      learningRate <- Seq(0.05)
    } yield {
      val neuralPrefs = NeuralPrefs(
        learningRate = learningRate,
        hiddenNodes = hiddenNodes,
        train = train,
        minibatchSize = 200,
        validation = validation,
        histogram = true,
        epochs = 5
      )
      val net: MultiLayerNetwork = trainNetwork(c, neuralPrefs)
      Log.v(neuralPrefs)
      NeuralModelLoader.save(net, c, Config.count)
      System.gc()
    }
  }

  def cacheMinimalArticles() = {
    val minimal = RddFetcher.annotatedRdd
      .filter(_.iptc.nonEmpty)
      .map(_.filterAnnotation(an => an.fb != Annotation.NONE && W2VLoader.contains(an.fb)))
      .filter(_.ann.nonEmpty)
      .map(a => f"${a.id} ${a.iptc.map(IPTC.trim).mkString(",")} ${a.ann.map(_._2.fb).mkString(",")}")
    saveAsText(minimal, "minimal")
  }

  def cacheBalanced() = {
    val train = RddFetcher.annotatedTrainW2V
    val validation = RddFetcher.annotatedValidationW2V
    val test = RddFetcher.annotatedTestW2V
    val splits = Seq("train" -> train, "validation" -> validation, "test" -> test)
    splits.foreach { case (kind, rdd) =>
      rdd.cache()
      IPTC.topCategories
        //      Set("weather")
        .foreach(c => {
        val balanced = RddFetcher.createBalanced(c, rdd).filter(_.iptc.nonEmpty)
        SparkUtil.saveAsText(balanced.map(JsonSingle.toSingleJson), s"${IPTC.trim(c)}_${kind}_balanced")
      })
    }
  }

  def splitAndCacheArticlesWithW2V() = {
    val rdd = RddFetcher.annotatedRdd
      .filter(_.iptc.nonEmpty)
      .map(_.filterAnnotation(an => an.fb != Annotation.NONE && W2VLoader.contains(an.fb)))
      .filter(_.ann.nonEmpty)
      .map(JsonSingle.toSingleJson)
    val splits = rdd.sortBy(a => Math.random).randomSplit(Array(0.6, 0.2, 0.2), Config.seed)
    saveAsText(splits(0), "nyt_train_w2v_" + Config.minimumAnnotations)
    saveAsText(splits(1), "nyt_validation_w2v_" + Config.minimumAnnotations)
    saveAsText(splits(2), "nyt_test_w2v_" + Config.minimumAnnotations)
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

