package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.{NeuralModel, NeuralModelLoader, NeuralPredictor}
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.NeuralEvaluation
import no.habitats.corpus.dl4j.networks.{FeedForwardIterator, RNNIterator}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.util.Try

object Tester {

  def tester(name: String) = name match {
    case _ if name.contains("nb") => NaiveBayesTester(name)
    case _ if name.contains("rnn") => RecurrentTester(name)
    case _ if name.contains("ffn") => FeedforwardTester(name)
    case _ => throw new IllegalStateException(s"Illegal model: $name")
  }

  def testModels() = {
    Config.resultsFileName = "test_all.txt"
    Config.resultsCatsFileName = "test_all.txt"
    Log.h("Testing models")
    val test = Fetcher.annotatedTestOrdered.map(_.toMinimal)
    tester("all-ffn-bow").test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-ffn-w2v").test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-rnn-w2v").test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-nb-bow").test(test)
    tester("all-nb-w2v").test(test)
  }

  def verifyAll(): Boolean = new File(Config.modelPath).listFiles().map(_.getName).map(tester).forall(_.verify)

  def testSub() = {
    Config.resultsFileName = "test_sub.txt"
    Config.resultsCatsFileName = "test_sub.txt"
    Log.h("Testing sub models")
    val test = Fetcher.subTestOrdered.map(_.toMinimal)
    val includeExampleBased = true
    tester("sub-rnn-w2v").test(test, includeExampleBased)
    tester("sub-ffn-w2v").test(test, includeExampleBased)
    tester("sub-ffn-bow").test(test, includeExampleBased)
    tester("sub-nb-bow").test(test, includeExampleBased)
    tester("sub-nb-w2v").test(test, includeExampleBased)
  }

  def testEmbeddedVsBoW() = {
    Config.resultsFileName = "test_embedded_vs_bow.txt"
    Config.resultsCatsFileName = "test_embedded_vs_bow_cats.txt"
    Log.h("Testing embedded vs. BoW")
    val test = Fetcher.annotatedTestOrdered.map(_.toMinimal)

    val predict = true
    tester("all-ffn-bow").test(test, predict)
    tester("all-nb-bow").test(test, predict)
    tester("all-ffn-w2v").test(test, predict)
    tester("all-nb-w2v").test(test, predict)
  }

  def testFFNBow() = {
    Config.resultsFileName = "test_embedded_vs_bow.txt"
    Config.resultsCatsFileName = "test_embedded_vs_bow.txt"
    Log.h("Testing embedded vs. BoW")
    val test = Fetcher.annotatedTestOrdered

    tester("ffn-bow-all").test(test)
    tester("ffn-w2v-all").test(test)
  }

  def testTypesInclusion() = {
    Config.resultsFileName = "test_type_inclusion.txt"
    Config.resultsCatsFileName = "test_type_inclusion.txt"
    Log.h("Testing type inclusion")
    val rddOrdered = Fetcher.subTestOrdered.map(_.toMinimal)

    Log.r("Annotations with types ...")
    tester("types-ffn-w2v").test(rddOrdered)
    tester("ffn-w2v").test(rddOrdered)

    //    Log.r("Normal annotations ...")
    //    FeedforwardTester("ffn-w2v-ordered").test(testOrdered)
  }

  def testShuffledVsOrdered() = {

    Config.resultsFileName = "test_shuffled_vs_ordered.txt"
    Config.resultsCatsFileName = "test_shuffled_vs_ordered.txt"
    Log.h("Testing shuffled vs. ordered")

    // Shuffled
    Log.r("Shuffled ...")
    val rddShuffled = Fetcher.annotatedTestShuffled.map(_.toMinimal)
    FeedforwardTester("ffn-w2v-shuffled").test(rddShuffled)

    //    // Ordered
    //    Log.r("Ordered ...")
    //    val rddOrdered = Fetcher.annotatedTestOrdered
    //    val testOrdered = rddOrdered.collect()
    //    FeedforwardTester("ffn-w2v-ordered").test(testOrdered)
  }

  def testLengths() = {
    Config.resultsFileName = "test_lengths.txt"
    Config.resultsCatsFileName = "test_lengths_cats.txt"
    Log.h("Testing Lengths")
    testBuckets("length", tester("ffn-w2v-ordered"), _.wc)
    testBuckets("length", tester("nb-bow"), _.id.toInt)
    testBuckets("length", tester("nb-w2v"), _.id.toInt)
  }

  def testTimeDecay() = {
    Config.resultsFileName = "test_time_decay.txt"
    Config.resultsCatsFileName = "test_time_decay_cats.txt"
    Log.h("Testing Time Decay")
    //    testBuckets("time", FeedforwardTester("ffn-w2v-time-all"), _.id.toInt)
    testBuckets("time", tester("ffn-bow-time-all"), _.id.toInt)
    //    testBuckets("time", NaiveBayesTester("nb-bow"), _.id.toInt)
    //    testBuckets("time", NaiveBayesTester("nb-w2v"), _.id.toInt)
  }

  def testConfidence() = {
    Config.resultsFileName = "test_confidence.txt"
    Config.resultsCatsFileName = "test_confidence_cats.txt"
    Log.h("Testing Confidence Levels")
    for {confidence <- Seq(25, 50, 75, 100)}
      yield FeedforwardTester(s"ffn-w2v-ordered-confidence-${confidence}").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"))
  }

  /** Test model on every test set matching name */
  def testBuckets(name: String, tester: Testable, criterion: Article => Double) = {
    val rdds: Array[(Int, RDD[Article])] = new File(Config.dataPath + s"nyt/$name/").listFiles
      .map(_.getName).filter(_.contains(s"test"))
      .map(n => (n.split("_").filter(s => Try(s.toInt).isSuccess).head.toInt, n))
      .map { case (index, n) => (index, Fetcher.fetch(s"nyt/$name/$n")) }
      .sortBy(_._1)
    rdds.foreach { case (index, n) => {
      Log.r2(s"${name} group: $index -  min: ${Try(n.map(criterion).min).getOrElse("N/A")} - max: ${Try(n.map(criterion).max).getOrElse("N/A")}")
      tester.test(n, iteration = index)
    }
    }
  }
}

sealed trait Testable {

  import scala.collection.JavaConverters._

  val modelName: String

  def iter(test: Array[Article], label: String): DataSetIterator = ???

  def models(modelName: String): Map[String, NeuralModel] = ???

  def verify: Boolean = Try(models(modelName)).isSuccess

  def test(test: RDD[Article], predict: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    val predictedArticles: Option[RDD[Article]] = if (predict) Some(predicted(test)) else None
    NeuralEvaluation.log(labelBased(test, iteration), IPTC.topCategories, iteration, predictedArticles)
    if (shouldLogResults) predictedArticles.foreach(logResults)
    System.gc()
  }

  def logResults(articles: RDD[Article]) = {
    articles.sortBy(_.id).foreach(a => {
      a.pred.foreach(p => {
        val folder: String = Config.dataPath + s"res/predictions/$modelName/" + (if (a.iptc.contains(p)) "tp" else "fp")
        Log.toFile(a.toStringFull, p, folder)
      })
    })
  }

  def predicted(test: RDD[Article]): RDD[Article] = {
    val modelType = if (modelName.toLowerCase.contains("bow")) Some(TFIDF.deserialize(modelName)) else None
    NeuralPredictor.predict(test, models(modelName), modelType)
  }

  def labelBased(test: RDD[Article], iteration: Int): Seq[NeuralEvaluation] = {
    if (iteration == 0) Log.r(s"Testing $modelName ...")
    models(modelName).toSeq.sortBy(_._1).zipWithIndex.map { case (models, i) => {
      val ffnTest = iter(test.collect(), models._1)
      val rnnEval = NeuralEvaluation(models._2.network, ffnTest.asScala, i, models._1)
      rnnEval.log()
      rnnEval
    }
    }
  }
}

case class FeedforwardTester(modelName: String) extends Testable {
  lazy val ffa: Map[String, NeuralModel] = NeuralModelLoader.models(modelName)

  override def iter(test: Array[Article], label: String): DataSetIterator = new FeedForwardIterator(test, label, 500, if (modelName.contains("bow")) Some(TFIDF.deserialize(modelName)) else None)
  override def models(modelName: String): Map[String, NeuralModel] = ffa
}

case class RecurrentTester(modelName: String) extends Testable {
  lazy val rnn: Map[String, NeuralModel] = NeuralModelLoader.models(modelName)

  override def iter(test: Array[Article], label: String): DataSetIterator = new RNNIterator(test, Some(label), 50)
  override def models(modelName: String): Map[String, NeuralModel] = rnn
}

case class NaiveBayesTester(modelName: String) extends Testable {
  lazy val nb: Map[String, NaiveBayesModel] = IPTC.topCategories.map(c => (c, MLlibModelLoader.load(modelName, IPTC.trim(c)))).toMap

  override def verify: Boolean = Try(nb).isSuccess

  override def test(articles: RDD[Article], includeExampleBased: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    Log.r(s"Testing Naive Bayes [$modelName] ...")
    val predicted = MlLibUtils.testMLlibModels(articles, nb, if (modelName.contains("bow")) Some(TFIDF.deserialize(modelName)) else None)

    Log.v("--- Predictions complete! ")
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs()))
    if (shouldLogResults) logResults(predicted)
    System.gc()
  }
}
