package no.habitats.corpus.spark

import java.io.File

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
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

/**
  * Created by mail on 03.05.2016.
  */
object Tester {

  def testModels() = {
    Config.resultsFileName = "res_all.txt"
    Config.resultsCatsFileName = "res_all.txt"
    val rdd = Fetcher.subTestW2V
    rdd.cache()
    val test = rdd.collect()
    testRNN(test, "rnn-w2v-sub-10")
    //    testRNN(test, "rnn-w2v-balanced-10")
    testFFN(test, "ffn-w2v")
    //    testNaiveBayes(rdd, "nb-bow")
    //    testNaiveBayes(rdd, "nb-w2v")
  }

  def testLengths() = {
    Config.resultsFileName = "res_lengths.txt"
    Config.resultsCatsFileName = "res_lengths.txt"
    testBuckets("length", testFFN)
  }

  def testTimeDecay() = {
    Config.resultsFileName = "res_time.txt"
    Config.resultsCatsFileName = "res_time.txt"
    testBuckets("time", testFFN)
  }

  /** Test model on every test set matching name */
  def testBuckets(name: String, test: (Array[Article], String) => Unit) = {
    val rdds: Array[(Int, RDD[Article])] = new File(Config.dataPath + "nyt").listFiles
      .map(_.getName).filter(_.contains(s"test_$name"))
      .map(n => (n.filter(_.isDigit).head.toString.toInt, n))
      .map { case (k, v) => (k, Fetcher.fetch(s"nyt/$v")) }
    rdds.foreach { case (k, v) => {
      val collect: Array[Article] = v.collect()
      Log.r(s"${name} group: $k -  min: ${collect.map(_.body.length).min} - max: ${collect.map(_.body.length).max}")
      test(collect, "ffn-w2v")
    }
    }
  }

  def testNaiveBayes(rdd: RDD[Article], name: String): Map[String, NaiveBayesModel] = {
    Log.r(s"Testing Naive Bayes [$name] ...")
    val nb: Map[String, NaiveBayesModel] = IPTC.topCategories.map(c => (c, MLlibModelLoader.load(name, IPTC.trim(c)))).toMap
    val phrases: Array[String] = Config.dataFile(Config.modelPath + "nb_phrases.txt").getLines().toArray.sorted
    val prefs = sc.broadcast(Prefs())
    MlLibUtils.testMLlibModels(rdd, nb, phrases, prefs, name.toLowerCase.contains("bow"))
  }

  def testFFN(test: Array[Article], modelName: String) = {
    Log.r(s"Testing FFN [$modelName] ...")
    val ffa: Map[String, MultiLayerNetwork] = NeuralModelLoader.models(modelName)
    val evals: Set[NeuralEvaluation] = ffa.toSeq.sortBy(_._1).zipWithIndex.map { case (models, i) => {
      val ffnTest = new FeedForwardIterator(test, models._1, 500)
      val ffnEval = NeuralEvaluation(models._2, ffnTest, i, models._1)
      ffnEval.log()
      ffnEval
    }
    }.toSet
    NeuralEvaluation.log(evals, Config.cats)
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
    NeuralEvaluation.log(evals, Config.cats)
  }
}
