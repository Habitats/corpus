package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.{NeuralModel, NeuralModelLoader, NeuralPredictor}
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.{Article, CorpusDataset}
import no.habitats.corpus.dl4j.NeuralEvaluation
import no.habitats.corpus.dl4j.networks.{FeedForwardIterator, RNNIterator}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.DataSetIterator

import scala.util.Try

object Tester {

  def tester(name: String, directory: String) = name match {
    case _ if name.contains("nb") => NaiveBayesTester(name, directory)
    case _ if name.contains("rnn") => RecurrentTester(name, directory)
    case _ if name.contains("ffn") => FeedforwardTester(name, directory)
    case _ => throw new IllegalStateException(s"Illegal model: $name")
  }

  val baseline    = "baseline"
  val superSample = "superSample"
  val types       = "types"
  val time        = "time"
  val confidence  = "confidence"
  val length      = "length"

  def testModels() = {
    Log.v("Testing models")
    val test = Fetcher.annotatedTestOrdered.map(_.toMinimal)
    tester("all-ffn-bow", baseline).test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-ffn-w2v", baseline).test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-rnn-w2v", baseline).test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-nb-bow", baseline).test(test)
    tester("all-nb-w2v", baseline).test(test)
  }

  def testEmbeddedVsBoW() = {
    Log.v("Testing embedded vs. BoW")
    val test = Fetcher.annotatedTestOrdered.map(_.toMinimal)

    val predict = true
    tester("all-ffn-bow", baseline).test(test, predict)
    tester("all-nb-bow", baseline).test(test, predict)
    tester("all-ffn-w2v", baseline).test(test, predict)
    tester("all-nb-w2v", baseline).test(test, predict)
  }

  def testTypesInclusion() = {
    Log.v("Testing type inclusion")
    val rddOrdered = Fetcher.subTestOrdered.map(_.toMinimal)

    Log.v("Annotations with types ...")
    tester("types-ffn-w2v", types).test(rddOrdered)
    tester("ffn-w2v", types).test(rddOrdered)

    //    Log.r("Normal annotations ...")
    //    FeedforwardTester("ffn-w2v-ordered").test(testOrdered)
  }

  def testLengths() = {
    Log.v("Testing Lengths")
    //    testBuckets("_length", tester("_baseline/nb_bow_all"), _.wc)
    //    testBuckets(length, tester("nb_w2v_all", baseline), _.wc)
    //    testBuckets(length, tester("ffn_w2v_all", baseline), _.wc)
    //    Try(testBuckets(length, tester("ffn_bow_all", baseline), _.wc))
    Try(testBuckets(length, tester("rnn_w2v_all", baseline), _.id.toInt))
  }

  def testTimeDecay() = {
    Log.v("Testing Time Decay")
    //    testBuckets(time, tester("ffn_w2v_all", time), _.id.toInt)
    //    Try(testBuckets(time, tester("ffn_bow_all", time), _.id.toInt))
    //            testBuckets(time, tester("nb_bow_all", time), _.id.toInt)
    //        testBuckets(time, tester("nb_w2v_all", time), _.id.toInt)
    //    testBuckets(time, tester("nb_w2v_all", time), _.id.toInt)
    Try(testBuckets(time, tester("rnn_w2v_all", time), _.id.toInt))
  }

  def testConfidence() = {
    Log.v("Testing Confidence Levels")
    for (conf <- Seq(25, 50, 75, 100)) {
      //      tester(s"confidence-${confidence}_ffn_bow_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"), predict = true)
      tester(s"confidence-${conf}_ffn_w2v_all", confidence).test(Fetcher.by(s"confidence/nyt_mini_test_ordered_$conf.txt"), predict = true)
      tester(s"confidence-${conf}_nb_w2v_all", confidence).test(Fetcher.by(s"confidence/nyt_mini_test_ordered_$conf.txt"))
      tester(s"confidence-${conf}_nb_bow_all", confidence).test(Fetcher.by(s"confidence/nyt_mini_test_ordered_$conf.txt"))
    }
  }

  /** Test model on every test set matching name */
  def testBuckets(tag: String, tester: Testable, criterion: Article => Double) = {
    val start = System.currentTimeMillis()
    val name = tester.name
    val resFile: String = tester.testDir + s"$name.txt"
    val resFileLabels: String = tester.testDir + s"${name}_labels.txt"
    Log.toFile("", resFile)
    Log.toFile(s"Testing $name - ${Config.getArgs}", resFile)
    Log.toFile("", resFile)
    Log.toFile("", resFileLabels)
    Log.toFile(s"Testing $name - ${Config.getArgs}", resFileLabels)
    Log.toFile("", resFileLabels)
    val testFiles: Array[File] = new File(Config.dataPath + s"nyt/$tag/").listFiles
    Log.v("Test folder found: " + testFiles.map(_.getName).mkString(", "))
    val rdds: Array[(Int, RDD[Article])] = testFiles
      .map(_.getName).filter(_.contains(s"test"))
      .map(n => (n.split("_|-|\\.").filter(s => Try(s.toInt).isSuccess).last.toInt, n))
      .map { case (index, n) => (index, Fetcher.fetch(s"nyt/$tag/$n", 0.2)) }
      .sortBy(_._1)
    rdds.foreach { case (index, n) => {
      Log.toFile(s"$tag group: $index -  min: ${Try(n.map(criterion).min).getOrElse("N/A")} - max: ${Try(n.map(criterion).max).getOrElse("N/A")}", resFileLabels)
      tester.test(n, iteration = index)
    }
    }
    Log.toFile(s"Testing finished in ${SparkUtil.prettyTime(System.currentTimeMillis() - start)}", resFile)
  }
}

sealed trait Testable {

  import scala.collection.JavaConverters._

  val name: String
  val tag : String

  val modelDir: String = Config.modelDir(name, tag)
  val testDir : String = Config.testDir(name, tag)

  def iter(test: CorpusDataset, label: String): DataSetIterator = ???

  def models(modelName: String): Map[String, NeuralModel] = ???

  def verify: Boolean = Try(models(name)).isSuccess

  def test(test: RDD[Article], predict: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    test.persist()
    Log.v(s"Testing ${test.count} articles ...")
    val predictedArticles: Option[RDD[Article]] = if (predict) Some(predictAll(test)) else None

    val labelEvals: Seq[NeuralEvaluation] = labelBased(test, iteration)
    NeuralEvaluation.logLabelStats(labelEvals, testDir + s"${name}_labels.txt")
    NeuralEvaluation.log(labelEvals, testDir + s"$name.txt", IPTC.topCategories, iteration, predictedArticles)
    if (shouldLogResults) predictedArticles.foreach(logResults)
    test.unpersist()
    System.gc()
  }

  def logResults(articles: RDD[Article]) = {
    articles.sortBy(_.id).foreach(a => {
      a.pred.foreach(p => {
        val folder: String = Config.dataPath + s"predictions/$name/" + (if (a.iptc.contains(p)) "tp" else "fp")
        Log.saveToFile(a.toStringFull, folder + p)
      })
    })
  }

  def dataset(test: RDD[Article]): CorpusDataset = {
    val tfidf = TFIDF.deserialize(modelDir)
    name match {
      case e: String if e.contains("rnn") => CorpusDataset.genW2VMatrix(test, tfidf)
      case e: String if e.contains("bow") => CorpusDataset.genBoWDataset(test, tfidf)
      case e: String if e.contains("w2v") => CorpusDataset.genW2VDataset(test, tfidf)
    }
  }

  def predictAll(test: RDD[Article]): RDD[Article] = {
    val modelType = TFIDF.deserialize(modelDir)
    NeuralPredictor.predict(test, models(name), modelType)
  }

  def labelBased(test: RDD[Article], iteration: Int): Seq[NeuralEvaluation] = {
    if (iteration == 0) Log.toFile(s"Testing $name ...", testDir + s"$name.txt")
    val testDataset: CorpusDataset = dataset(test)
    val m = models(name) //.par
    //    m.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(Config.parallelism))
    m.zipWithIndex.map { case (models, i) => {
      val test = iter(testDataset, models._1)
      val eval = NeuralEvaluation(test, models._2.network, i, models._1)
      (i, eval)
    }
    }.seq.toSeq.sortBy(_._1).map(_._2)
  }
}

case class FeedforwardTester(name: String, tag: String) extends Testable {
  lazy val ffa: Map[String, NeuralModel] = NeuralModelLoader.models(modelDir)

  override def iter(test: CorpusDataset, label: String): DataSetIterator = new FeedForwardIterator(test, IPTC.topCategories.indexOf(label), 10000)
  override def models(modelName: String): Map[String, NeuralModel] = ffa
}

case class RecurrentTester(name: String, tag: String) extends Testable {
  lazy val rnn: Map[String, NeuralModel] = NeuralModelLoader.models(modelDir)

  override def iter(test: CorpusDataset, label: String): DataSetIterator = new RNNIterator(test, label, 500)
  override def models(modelName: String): Map[String, NeuralModel] = rnn
}

case class NaiveBayesTester(name: String, tag: String) extends Testable {
  lazy val nb: Map[String, NaiveBayesModel] = IPTC.topCategories.map(c => (c, MLlibModelLoader.load(modelDir, IPTC.trim(c)))).toMap

  override def verify: Boolean = Try(nb).isSuccess

  override def test(articles: RDD[Article], includeExampleBased: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    val predicted = MlLibUtils.testMLlibModels(articles, nb, TFIDF.deserialize(modelDir))

    Log.v("--- Predictions complete! ")
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs(iteration = iteration)), testDir + s"$name.txt", testDir + s"${name}_labels.txt")
    if (shouldLogResults) logResults(predicted)
    System.gc()
  }
}
