package no.habitats.corpus.spark

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.dl4j.networks.{FeedForwardIterator, RNNIterator}
import no.habitats.corpus.dl4j.{FreebaseW2V, NeuralEvaluation, NeuralPrefs, TSNE}
import no.habitats.corpus.models.{Annotation, Article}
import no.habitats.corpus.npl.{IPTC, Spotlight, WikiData}
import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.util.Try

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

      case "cacheAnnotated" => annotateAndCacheArticles()
      case "splitAndCacheAnnotatedW2V" => splitAndCacheArticlesWithW2V() // REQUIREMENT FOR TRAINING
      case "cacheBalanced" => cacheBalanced()
      case "cacheMinimal" => cacheMinimalArticles()
      case "cacheSuperSampled" => RddFetcher.cacheSuperSampled(Some(100000))
      case "cacheSubSampled" => cacheSubSampled()

      // Display stats
      case "iptcDistribution" => calculateIPTCDistribution()
      case "tnesDocumentVectors" => tnesDocumentVectors()
      case "tnesWordVectors" => tnesWordVectors()

      // Modelling
      case "trainNaiveBayesBoW" => trainNaiveBayes(bow = true)
      case "trainNaiveBayesW2V" => trainNaiveBayes(bow = false)
      case "trainRNNSubSampled" => trainRNNSubSampled()
      case "trainFFNSubSampled" => trainFFNSubSampled()
      case "trainFFNSubSampledBoW" => trainFFNSubSampledBoW()
      case "trainRNNBalanced" => trainRNNBalanced()
      case "trainFFNBalanced" => trainFFNBalanced()
      case "trainRNNSpark" => trainRNNSpark()
      case "trainFFNSpark" => trainFFNSpark()
      case "testModels" => testModels()

      case _ => Log.r("No job ... Exiting!")
    }
    Log.r(s"Job completed in${prettyTime(System.currentTimeMillis - s)}")
    //    Thread.sleep(Long.MaxValue)
    //    sc.stop
  }

  lazy val cats: Seq[String] = Try(Seq(Config.category)).getOrElse(IPTC.topCategories)

  def trainRNNSpark()= {
    val train = RddFetcher.subTrainW2V
    val validation = RddFetcher.subValidationW2V
    Config.resultsFileName = "res_rnn.txt"
    Config.resultsCatsFileName = "res_rnn_cats.txt"
    val prefs = NeuralPrefs(train = train, validation = validation)
    FreebaseW2V.trainSparkRNN("sport", prefs)
  }

  def trainFFNSpark()= {
    val train = RddFetcher.subTrainW2V
    val validation = RddFetcher.subValidationW2V
    Config.resultsFileName = "res_ffn.txt"
    Config.resultsCatsFileName = "res_ffn_cats.txt"
    val prefs = NeuralPrefs(train = train, validation = validation)
    FreebaseW2V.trainSparkFFN("sport", prefs)
  }

  def trainRNNSubSampled() = {
    val train = RddFetcher.subTrainW2V
    val validation = RddFetcher.subValidationW2V
    Config.resultsFileName = "res_rnn.txt"
    Config.resultsCatsFileName = "res_rnn_cats.txt"
    cats.foreach(c => trainNeuralNetwork(c, FreebaseW2V.trainBinaryRNN, train, validation))
  }

  def trainRNNBalanced() = {
    Config.resultsFileName = "res_rnn.txt"
    Config.resultsCatsFileName = "res_rnn_cats.txt"
    cats.foreach(c => {
      val train = RddFetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = RddFetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, FreebaseW2V.trainBinaryRNN, train, validation)
    })
  }

  def trainFFNSubSampled() = {
    val train = RddFetcher.subTrainW2V
    val validation = RddFetcher.subValidationW2V
    Config.resultsFileName = "res_ffn.txt"
    Config.resultsCatsFileName = "res_ffn_cats.txt"
    cats.foreach(c => trainNeuralNetwork(c, FreebaseW2V.trainBinaryFFN, train, validation))
  }

  def trainFFNBalanced() = {
    Config.resultsFileName = "res_ffn.txt"
    Config.resultsCatsFileName = "res_ffn_cats.txt"
    cats.foreach(c => {
      val train = RddFetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = RddFetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, FreebaseW2V.trainBinaryRNN, train, validation)
    })
  }

  def trainNeuralNetwork(c: String = Config.category, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, train: RDD[Article], validation: RDD[Article]) = {
    for {
      hiddenNodes <- Seq(200)
//      hiddenNodes <- Seq(10)
      learningRate <- Seq(0.05)
      minibatchSize = 100
      histogram = false
      epochs = 5
    } yield {
      val neuralPrefs = NeuralPrefs(learningRate = learningRate, hiddenNodes = hiddenNodes, train = train, validation = validation, minibatchSize = minibatchSize, histogram = histogram, epochs = epochs)
      Log.r(neuralPrefs)
      val net: MultiLayerNetwork = trainNetwork(c, neuralPrefs)
      NeuralModelLoader.save(net, c, Config.count)
      System.gc()
    }
  }

  def trainFFNSubSampledBoW() = {
    val train = RddFetcher.subTrainW2V
    val validation = RddFetcher.subValidationW2V
    Config.resultsFileName = "res_ffn_bow.txt"
    Config.resultsCatsFileName = "res_ffn_bow_cats.txt"
    val phrases: Array[String] = (train ++ validation).flatMap(_.ann.keySet).collect.distinct.sorted
    cats.foreach(c => {
      for {
        hiddenNodes <- Seq(10)
        //      hiddenNodes <- Seq(1, 5, 10, 20, 50, 100, 200)
        learningRate <- Seq(0.05)
        minibatchSize = 50
        histogram = false
        epochs = 5
      } yield {
        val neuralPrefs = NeuralPrefs(learningRate = learningRate, hiddenNodes = hiddenNodes, train = train, validation = validation, minibatchSize = minibatchSize, histogram = histogram, epochs = epochs)
        Log.r(neuralPrefs)
        val net: MultiLayerNetwork = FreebaseW2V.trainBinaryFFNBoW(c, neuralPrefs, phrases)
        NeuralModelLoader.save(net, c, Config.count)
        System.gc()
      }
    })
  }

  def trainNaiveBayes(bow: Boolean) = {
    Config.resultsFileName = "res_nb.txt"
    Config.resultsCatsFileName = "res_nb_cats.txt"
    //    val train = RddFetcher.annotatedTrainW2V
    //    val validation = RddFetcher.annotatedValidationW2V
    val train = RddFetcher.subTrainW2V
    val validation = RddFetcher.subValidationW2V
    val prefs = sc.broadcast(Prefs())
    val phrases: Array[String] = (train ++ validation).flatMap(_.ann.keySet).collect.distinct.sorted
    Log.toFile(phrases, "nb_phrases.txt", Config.modelPath)
    val models = ML.multiLabelClassification(prefs, train, validation, phrases, bow)
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"nb_${if(bow) "bow" else "w2v"}_${IPTC.trim(c)}.bin") }
  }

  def testModels() = {
    Config.resultsFileName = "res_all.txt"
    Config.resultsCatsFileName = "res_all.txt"
    val rdd = RddFetcher.subTestW2V
    rdd.cache()
    val test = rdd.collect()
//    testRNN(test, "rnn-w2v-sub-10")
//    testRNN(test, "rnn-w2v-balanced-10")
//    testFFN(test, "ffn-w2v")
//    testNaiveBayes(rdd, "nb-bow")
//    testNaiveBayes(rdd, "nb-w2v")
  }

  def testNaiveBayes(rdd: RDD[Article], name: String): Map[String, NaiveBayesModel] = {
    Log.r(s"Testing Naive Bayes [$name] ...")
    val nb: Map[String, NaiveBayesModel] = IPTC.topCategories.map(c => (c, MLlibModelLoader.load(name, IPTC.trim(c)))).toMap
    val phrases: Array[String] = Config.dataFile(Config.modelPath + "nb_phrases.txt").getLines().toArray.sorted
    val prefs = sc.broadcast(Prefs())
    ML.testModels(rdd, nb, phrases, prefs, name.toLowerCase.contains("bow"))
  }

  def testFFN(test: Array[Article], name: String) = {
    Log.r(s"Testing FFN [$name] ...")
    val ffa: Map[String, MultiLayerNetwork] = NeuralModelLoader.models(name)
    val evals: Set[NeuralEvaluation] = ffa.toSeq.sortBy(_._1).zipWithIndex.map { case (models, i) => {
      val ffnTest = new FeedForwardIterator(test, models._1, 500)
      val ffnEval = NeuralEvaluation(models._2, ffnTest, i, models._1)
      ffnEval.log()
      ffnEval
    }
    }.toSet
    NeuralEvaluation.log(evals, cats)
  }

  def testRNN(test: Array[Article], name: String) = {
    Log.r(s"Testing RNN [$name] ...")
    val rnn: Map[String, MultiLayerNetwork] = NeuralModelLoader.models(name)
    val evals: Set[NeuralEvaluation] = rnn.toSeq.sortBy(_._1).zipWithIndex.map { case (models, i) => {
      val ffnTest = new RNNIterator(test, Some(models._1), 50)
      val rnnEval = NeuralEvaluation(models._2, ffnTest, i, models._1)
      rnnEval.log()
      rnnEval
    }
    }.toSet
    NeuralEvaluation.log(evals, cats)
  }

  def tnesDocumentVectors() = {
    TSNE.create(RddFetcher.subTrainW2V, useDocumentVectors = true)
  }

  def tnesWordVectors() = {
    TSNE.create(RddFetcher.subTrainW2V, useDocumentVectors = false)
  }

  /** Fetch json RDD and compute IPTC and annotations */
  def annotateAndCacheArticles() = {
    val rdd = RddFetcher.rdd
      .map(Corpus.toIPTC)
      .map(Corpus.toDBPediaAnnotated)
    saveAsText(rdd.map(JsonSingle.toSingleJson), "nyt_with_all")
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
    var rdd = RddFetcher.annotatedRdd
      .filter(_.iptc.nonEmpty)
      .map(_.filterAnnotation(an => an.fb != Annotation.NONE && W2VLoader.contains(an.fb)))
      .filter(_.ann.nonEmpty)
    rdd = TC(rdd).computed

    val splits = rdd.map(JsonSingle.toSingleJson).sortBy(a => Math.random).randomSplit(Array(0.6, 0.2, 0.2), Config.seed)
    saveAsText(splits(0), "nyt_train_w2v_" + Config.minimumAnnotations)
    saveAsText(splits(1), "nyt_validation_w2v_" + Config.minimumAnnotations)
    saveAsText(splits(2), "nyt_test_w2v_" + Config.minimumAnnotations)
  }

  def cacheSubSampled() = {
    val rdds = Map(
      "train" -> RddFetcher.annotatedTrainW2V,
      "test" -> RddFetcher.annotatedTestW2V,
      "validation" -> RddFetcher.annotatedValidationW2V
    )
    rdds.foreach { case (k, v) => RddFetcher.cacheSubSampled(v, k) }
  }

  def computeAndCacheDBPediaAnnotationsToJson() = {
    Spotlight.cacheDbpedia(RddFetcher.rdd, 0.5)
    Spotlight.cacheDbpedia(RddFetcher.rdd, 0.75)
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

  def stats(rdd: RDD[Article]) = {
    val a = Preprocess.preprocess(sc.broadcast(Prefs()), rdd)
    val prefs = sc.broadcast(Prefs(iteration = iter))
    val annCounts = a.flatMap(a => a.iptc.map(c => (c, a.ann.size))).reduceByKey(_ + _).collectAsMap
    Log.toFile(annCounts.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "stats/annotation_pr_iptc.txt")
    val artByAnn = a.flatMap(a => a.iptc.map(c => (c, 1))).reduceByKey(_ + _).collectAsMap
    Log.toFile(artByAnn.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "stats/articles_pr_iptc.txt")
    val iptc = a.flatMap(_.iptc).distinct.collect
    val avgAnnIptc = iptc.map(c => (c, annCounts(c).toDouble / artByAnn(c))).toMap
    Log.toFile(avgAnnIptc.map(c => f"${c._1}%30s ${c._2}%10.0f").mkString("\n"), "stats/average_ann_pr_iptc.txt")
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

  def saveAsText(rdd: RDD[String], name: String) = {
    val path = Config.cachePath + s"${name.replaceAll("[,\\s+]+", "_")}"
    FileUtils.deleteDirectory(new File(path))
    rdd.coalesce(1, shuffle = true).saveAsTextFile(path)
    val file = new File(path + ".json")
    Files.move(new File(path + "/part-00000").toPath, file.toPath, StandardCopyOption.REPLACE_EXISTING)
    FileUtils.deleteDirectory(new File(path))
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

