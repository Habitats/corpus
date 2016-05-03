package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.{FreebaseW2V, NeuralPrefs}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.util.Try

/**
  * Created by mail on 03.05.2016.
  */
object Trainer {

  def trainRNNSpark() = {
    val train = Fetcher.subTrainW2V
    val validation = Fetcher.subValidationW2V
    Config.resultsFileName = "res_rnn.txt"
    Config.resultsCatsFileName = "res_rnn_cats.txt"
    val prefs = NeuralPrefs(train = train, validation = validation)
    FreebaseW2V.trainSparkRNN("sport", prefs)
  }

  def trainFFNSpark() = {
    val train = Fetcher.subTrainW2V
    val validation = Fetcher.subValidationW2V
    Config.resultsFileName = "res_ffn.txt"
    Config.resultsCatsFileName = "res_ffn_cats.txt"
    val prefs = NeuralPrefs(train = train, validation = validation)
    FreebaseW2V.trainSparkFFN("sport", prefs)
  }

  def trainRNNSubSampled() = {
    val train = Fetcher.subTrainW2V
    val validation = Fetcher.subValidationW2V
    Config.resultsFileName = "res_rnn.txt"
    Config.resultsCatsFileName = "res_rnn_cats.txt"
    Config.cats.foreach(c => trainNeuralNetwork(c, FreebaseW2V.trainBinaryRNN, train, validation))
  }

  def trainRNNBalanced() = {
    Config.resultsFileName = "res_rnn.txt"
    Config.resultsCatsFileName = "res_rnn_cats.txt"
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, FreebaseW2V.trainBinaryRNN, train, validation)
    })
  }

  def trainFFNSubSampled() = {
    val train = Fetcher.subTrainW2V
    val validation = Fetcher.subValidationW2V
    Config.resultsFileName = "res_ffn.txt"
    Config.resultsCatsFileName = "res_ffn_cats.txt"
    Config.cats.foreach(c => trainNeuralNetwork(c, FreebaseW2V.trainBinaryFFN, train, validation))
  }

  def trainFFNBalanced() = {
    Config.resultsFileName = "res_ffn.txt"
    Config.resultsCatsFileName = "res_ffn_cats.txt"
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, FreebaseW2V.trainBinaryRNN, train, validation)
    })
  }

  def trainNeuralNetwork(c: String = Config.category, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, train: RDD[Article], validation: RDD[Article]) = {
    for {
      hiddenNodes <- Seq(300)
      //      hiddenNodes <- Seq(20, 50, 100)
      learningRate <- Seq(0.05)
      minibatchSize = 100
      //            c <- cats
      histogram = false
      epochs = 10
    } yield {
      val neuralPrefs = NeuralPrefs(learningRate = learningRate, hiddenNodes = hiddenNodes, train = train, validation = validation, minibatchSize = minibatchSize, histogram = histogram, epochs = epochs)
      val net: MultiLayerNetwork = trainNetwork(c, neuralPrefs)
      NeuralModelLoader.save(net, c, Config.count)
      System.gc()
    }
  }

  def trainFFNSubSampledBoW() = {
    val train = Fetcher.subTrainW2V
    val validation = Fetcher.subValidationW2V
    Config.resultsFileName = "res_ffn_bow.txt"
    Config.resultsCatsFileName = "res_ffn_bow_cats.txt"
    val phrases: Array[String] = (train ++ validation).flatMap(_.ann.keySet).collect.distinct.sorted

    for {
    //        hiddenNodes <- Seq(10)
      hiddenNodes <- Seq(20, 50, 100)
      learningRate <- Seq(0.05, 0.025, 0.01, 0.005)
      minibatchSize = 100
      //            c <- cats
      c = "sport"
      histogram = false
      epochs = 1
    } yield {
      val neuralPrefs = NeuralPrefs(learningRate = learningRate, hiddenNodes = hiddenNodes, train = train, validation = validation, minibatchSize = minibatchSize, histogram = histogram, epochs = epochs)
      Log.r(neuralPrefs)
      val net: MultiLayerNetwork = FreebaseW2V.trainBinaryFFNBoW(c, neuralPrefs, phrases)
      NeuralModelLoader.save(net, c, Config.count)
      System.gc()
    }
  }

  def trainNaiveBayes(bow: Boolean) = {
    Config.resultsFileName = "res_nb.txt"
    Config.resultsCatsFileName = "res_nb_cats.txt"
    //    val train = RddFetcher.annotatedTrainW2V
    //    val validation = RddFetcher.annotatedValidationW2V
    val train = Fetcher.subTrainW2V
    val validation = Fetcher.subValidationW2V
    val prefs = sc.broadcast(Prefs())
    val phrases: Array[String] = (train ++ validation).flatMap(_.ann.keySet).collect.distinct.sorted
    Log.toFile(phrases, "nb_phrases.txt", Config.modelPath)
    val models = MlLibUtils.multiLabelClassification(prefs, train, validation, phrases, bow)
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"nb_${if (bow) "bow" else "w2v"}_${IPTC.trim(c)}.bin") }
  }
}
