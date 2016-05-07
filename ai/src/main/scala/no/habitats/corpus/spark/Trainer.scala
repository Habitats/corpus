package no.habitats.corpus.spark

import java.time.LocalDateTime

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralEvaluation, NeuralPrefs, NeuralTrainer}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.{AsyncDataSetIterator, DataSetIterator}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.nd4j.linalg.dataset.DataSet

import scala.collection.JavaConverters._

object Trainer {

  def trainRNNSubSpark() = {
    val train = Fetcher.subTrainOrdered
    val validation = Fetcher.subValidationOrdered
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = "train_rnn.txt"
    val prefs = NeuralPrefs(train = train, validation = validation)
    trainSparkRNN("sport", prefs)
  }

  def trainFFNSubSpark() = {
    val train = Fetcher.subTrainOrdered
    val validation = Fetcher.subValidationOrdered
    Config.resultsFileName = "train_ffn.txt"
    Config.resultsCatsFileName = "train_ffn.txt"
    val prefs = NeuralPrefs(train = train, validation = validation)
    trainSparkFFN("sport", prefs)
  }

  def trainRNNSubSampled() = {
    val train = Fetcher.subTrainOrdered
    val validation = Fetcher.subValidationOrdered
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = "train_rnn.txt"
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryRNN, train, validation))
  }

  def trainRNNBalanced() = {
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = "train_rnn.txt"
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, trainBinaryRNN, train, validation)
    })
  }

  def trainFFNSubOrdered() = {
    val train = Fetcher.subTrainOrdered
    val validation = Fetcher.subValidationOrdered
    Config.resultsFileName = "train_ffn_ordered.txt"
    Config.resultsCatsFileName = "train_ffn_ordered_cats.txt"
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 1)
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, prefs))
  }

  def trainFFNSubOrderedTypes() = {
    val train = Fetcher.subTrainOrderedTypes
    val validation = Fetcher.subValidationOrdered
    Config.resultsFileName = "train_ffn_ordered_types.txt"
    Config.resultsCatsFileName = "train_ffn_ordered_types_cats.txt"
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 1)
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, prefs))
  }

  def trainFFNSubShuffled() = {
    val train = Fetcher.subTrainShuffled
    val validation = Fetcher.subValidationShuffled
    Config.resultsFileName = "train_ffn_shuffled.txt"
    Config.resultsCatsFileName = "train_ffn_shuffled_cats.txt"
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 1)
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, prefs))
  }

  def trainFFNSubSampled() = {
    val train = Fetcher.subTrainOrdered
    val validation = Fetcher.subValidationOrdered
    Config.resultsFileName = "train_ffn.txt"
    Config.resultsCatsFileName = "train_ffn_cats.txt"
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, train, validation))
  }

  def trainFFNBalanced() = {
    Config.resultsFileName = "train_ffn.txt"
    Config.resultsCatsFileName = "train_ffn_cats.txt"
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, trainBinaryRNN, train, validation)
    })
  }

  def trainSparkRNN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    var net = RNN.createBinary(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val trainIter: List[DataSet] = new RNNIterator(neuralPrefs.train.collect(), Some(label), batchSize = neuralPrefs.minibatchSize).asScala.toList
    val testIter = new AsyncDataSetIterator(new RNNIterator(neuralPrefs.validation.collect(), Some(label), batchSize = neuralPrefs.minibatchSize))
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, 200, 2)
      val eval = NeuralEvaluation(net, testIter, i, label)
      eval.log()
      testIter.reset()
    }

    net
  }

  def trainSparkFFN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    var net = FeedForward.create(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val testIter: DataSetIterator = new FeedForwardIterator(neuralPrefs.validation.collect(), label, batchSize = neuralPrefs.minibatchSize)
    val trainIter: List[DataSet] = new FeedForwardIterator(neuralPrefs.train.collect(), label, batchSize = neuralPrefs.minibatchSize).asScala.toList
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, 200, 2)
      val eval = NeuralEvaluation(net, testIter, i, label)
      eval.log()
      testIter.reset()
    }

    net
  }

  def trainBinaryRNN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = RNN.createBinary(neuralPrefs)
    val trainIter = new RNNIterator(neuralPrefs.train.collect(), Some(label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(neuralPrefs.validation.collect(), Some(label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  def trainBinaryFFN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = FeedForward.create(neuralPrefs)
    val trainIter = new FeedForwardIterator(neuralPrefs.train.collect(), label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(neuralPrefs.validation.collect(), label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  def trainBinaryFFNBoW(label: String, neuralPrefs: NeuralPrefs, phrases: Array[String]): MultiLayerNetwork = {
    val net = FeedForward.createBoW(neuralPrefs, phrases.size)
    val trainIter = new FeedForwardIterator(neuralPrefs.train.collect(), label, batchSize = neuralPrefs.minibatchSize, phrases = phrases)
    val testIter = new FeedForwardIterator(neuralPrefs.validation.collect(), label, batchSize = neuralPrefs.minibatchSize, phrases = phrases)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  def trainNeuralNetwork(c: String, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, train: RDD[Article], validation: RDD[Article]): Unit = {
    for {
      hiddenNodes <- Seq(300)
      //      hiddenNodes <- Seq(20, 50, 100)
      learningRate <- Seq(0.05)
      minibatchSize = 100
      //            c <- cats
      histogram = false
      epochs = 1
    } yield {
      val neuralPrefs = NeuralPrefs(learningRate = learningRate, hiddenNodes = hiddenNodes, train = train, validation = validation, minibatchSize = minibatchSize, histogram = histogram, epochs = epochs)
      trainNeuralNetwork(c, trainNetwork, neuralPrefs)
    }
  }

  def trainNeuralNetwork(c: String, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, neuralPrefs: NeuralPrefs) = {
    val net: MultiLayerNetwork = trainNetwork(c, neuralPrefs)
    NeuralModelLoader.save(net, c + "_" + (System.currentTimeMillis()/1000).toString.substring(5), Config.count)
    System.gc()
  }

  def trainFFNSubSampledBoW() = {
    val train = Fetcher.subTrainOrdered
    val validation = Fetcher.subValidationOrdered
    Config.resultsFileName = "train_ffn_bow.txt"
    Config.resultsCatsFileName = "train_ffn_bow_cats.txt"
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
      val net: MultiLayerNetwork = trainBinaryFFNBoW(c, neuralPrefs, phrases)
      NeuralModelLoader.save(net, c, Config.count)
      System.gc()
    }
  }

  def trainNaiveBayes(bow: Boolean) = {
    Config.resultsFileName = "train_nb.txt"
    Config.resultsCatsFileName = "train_nb_cats.txt"
    //    val train = RddFetcher.annotatedTrainW2V
    //    val validation = RddFetcher.annotatedValidationW2V
    val train = Fetcher.subTrainOrdered
    val validation = Fetcher.subValidationOrdered
    val prefs = sc.broadcast(Prefs())
    val phrases: Array[String] = (train ++ validation).flatMap(_.ann.keySet).collect.distinct.sorted
    Log.toFile(phrases, "nb_phrases.txt", Config.modelPath)
    val models = MlLibUtils.multiLabelClassification(prefs, train, validation, phrases, bow)
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"nb_${if (bow) "bow" else "w2v"}_${IPTC.trim(c)}.bin") }
  }
}


