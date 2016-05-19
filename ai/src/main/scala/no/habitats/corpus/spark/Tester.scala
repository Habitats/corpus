package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus.TFIDF
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.NeuralEvaluation
import no.habitats.corpus.dl4j.networks.{FeedForwardIterator, RNNIterator}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.io.Source
import scala.util.Try

object Tester {

  def testModels() = {
    Config.resultsFileName = "test_all.txt"
    Config.resultsCatsFileName = "test_all.txt"
    Log.h("Testing models")
    val rdd = Fetcher.subTestOrdered.map(_.toMinimal)
    rdd.cache()
    val test = rdd.collect()
    RecurrentTester("rnn-w2v-sub-10").test(test)
    RecurrentTester("rnn-w2v-balanced-10").test(test)
    FeedforwardTester("ffn-w2v").test(test)

    NaiveBayesTester("nb-w2v").test(test)
    NaiveBayesTester("nb-bow").test(test)
  }

  def testEmbeddedVeBoW() = {
    Config.resultsFileName = "test_embedded_vs_bow.txt"
    Config.resultsCatsFileName = "test_embedded_vs_bow.txt"
    Log.h("Testing embedded vs. BoW")
    val rdd = Fetcher.subTestOrdered.map(_.toMinimal)
    val test = rdd.collect()

    RecurrentTester("rnn-w2v-sub-10").test(test)
    FeedforwardTester("ffn-w2v").test(test)
    NaiveBayesTester("nb-w2v").test(test)
    NaiveBayesTester("nb-bow").test(test)
  }

  def testFNNOrdered() = {

  }

  def testFFNBow() = {
    Config.resultsFileName = "test_embedded_vs_bow.txt"
    Config.resultsCatsFileName = "test_embedded_vs_bow.txt"
    Log.h("Testing embedded vs. BoW")
    val rdd = Fetcher.annotatedTestOrdered
    val test = rdd.collect()

    val name: String = "ffn-bow-10000"
    FeedforwardTester(name).test(test)
  }

  def testTypesInclusion() = {
    Config.resultsFileName = "test_type_inclusion.txt"
    Config.resultsCatsFileName = "test_type_inclusion.txt"
    Log.h("Testing type inclusion")
    val rddOrdered = Fetcher.subTestOrdered.map(_.toMinimal)
    val testOrdered = rddOrdered.collect()

    Log.r("Annotations with types ...")
    FeedforwardTester("ffn-w2v-ordered-types").test(testOrdered)

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
    val testShuffled = rddShuffled.collect()
    FeedforwardTester("ffn-w2v-shuffled").test(testShuffled)

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
    testBuckets("length", FeedforwardTester("ffn-w2v-ordered"), _.wc)
    testBuckets("length", NaiveBayesTester("nb-bow"), _.id.toInt)
    testBuckets("length", NaiveBayesTester("nb-w2v"), _.id.toInt)
  }

  def testTimeDecay() = {
    Config.resultsFileName = "test_time_decay.txt"
    Config.resultsCatsFileName = "test_time_decay_cats.txt"
    Log.h("Testing Time Decay")
    testBuckets("time", FeedforwardTester("ffn-time"), _.id.toInt)
    //    testBuckets("time", NaiveBayesTester("nb-bow"), _.id.toInt)
    //    testBuckets("time", NaiveBayesTester("nb-w2v"), _.id.toInt)
  }

  def testConfidence() = {
    Config.resultsFileName = "test_confidence.txt"
    Config.resultsCatsFileName = "test_confidence_cats.txt"
    Log.h("Testing Confidence Levels")
    for {confidence <- Seq(25, 50, 75, 100)}
      yield FeedforwardTester(s"ffn-w2v-ordered-confidence-${confidence}").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt").collect)
  }

  /** Test model on every test set matching name */
  def testBuckets(name: String, tester: Testable, criterion: Article => Double) = {
    val rdds: Array[(Int, RDD[Article])] = new File(Config.dataPath + s"nyt/$name/").listFiles
      .map(_.getName).filter(_.contains(s"test"))
      .map(n => (n.split("_").filter(s => Try(s.toInt).isSuccess).head.toInt, n))
      .map { case (index, n) => (index, Fetcher.fetch(s"nyt/$name/$n")) }
      .sortBy(_._1)
    rdds.foreach { case (index, n) => {
      val collect: Array[Article] = n.collect()
      Log.r2(s"${name} group: $index -  min: ${Try(collect.map(criterion).min).getOrElse("N/A")} - max: ${Try(collect.map(criterion).max).getOrElse("N/A")}")
      tester.test(collect, iteration = index)
    }
    }
  }
}

sealed trait Testable {
  val modelName: String

  def iter(test: Array[Article], label: String): DataSetIterator = ???

  def models(modelName: String): Map[String, MultiLayerNetwork] = ???

  def test(test: Array[Article], iteration: Int = 0) = {
    if (iteration == 0) Log.r(s"Testing $modelName ...")
    val evals: Set[NeuralEvaluation] = models(modelName).toSeq.sortBy(_._1).zipWithIndex.map { case (models, i) => {
      val ffnTest = iter(test, models._1)
      val rnnEval = NeuralEvaluation(models._2, ffnTest, i, models._1)
      rnnEval.log()
      rnnEval
    }
    }.toSet
    NeuralEvaluation.log(evals, Config.cats, iteration)
  }
}

case class FeedforwardTester(modelName: String) extends Testable {
  lazy val ffa: Map[String, MultiLayerNetwork] = NeuralModelLoader.models(modelName)

  override def iter(test: Array[Article], label: String): DataSetIterator = new FeedForwardIterator(test, label, 500, if(modelName.contains("bow")) Some(TFIDF.deserialize(modelName)) else None)
  override def models(modelName: String): Map[String, MultiLayerNetwork] = ffa
}

case class RecurrentTester(modelName: String) extends Testable {
  lazy val rnn: Map[String, MultiLayerNetwork] = NeuralModelLoader.models(modelName)

  override def iter(test: Array[Article], label: String): DataSetIterator = new RNNIterator(test, Some(label), 50)
  override def models(modelName: String): Map[String, MultiLayerNetwork] = rnn
}

case class NaiveBayesTester(modelName: String) extends Testable {

  def test(articles: Array[Article]) = {
    Log.r(s"Testing Naive Bayes [$modelName] ...")
    val nb: Map[String, NaiveBayesModel] = IPTC.topCategories.map(c => (c, MLlibModelLoader.load(modelName, IPTC.trim(c)))).toMap
    val rdd: RDD[Article] = sc.parallelize(articles)
    MlLibUtils.testMLlibModels(rdd, nb, if(modelName.contains("bow")) Some(TFIDF.deserialize(modelName)) else None, sc.broadcast(Prefs()))
  }
}
