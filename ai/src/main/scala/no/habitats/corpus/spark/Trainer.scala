package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralEvaluation, NeuralPrefs, NeuralTrainer}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs, Preprocess}
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.{AsyncDataSetIterator, DataSetIterator}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.nd4j.linalg.dataset.DataSet

import scala.collection.JavaConverters._

object Trainer extends NeuralTrainer{

  implicit def collect(rdd: RDD[Article]): Array[Article] = rdd.collect()

  def trainRNNSpark() = {
    val (train, validation) = Fetcher.ordered(false)
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(train = train, validation = validation)
    sparkRNNTrainer("sport", prefs)
  }

  def trainFFNSpark() = {
    val (train, validation) = Fetcher.ordered(false)
    Config.resultsFileName = "train_ffn_spark.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    for {
      lr <- Seq(0.075, 0.1, 0.2, 0.3, 0.4)
      mbs <- Seq(1000, 2000, 3000)
    } yield {
      val prefs = NeuralPrefs(learningRate = lr, train = train, validation = validation, minibatchSize = mbs, epochs = 10)
      Config.cats.foreach(c => trainNeuralNetwork(c, sparkFFNTrainer, prefs, "ffn-spark"))
    }
  }

  def trainFFNTime() = {
    val train = Fetcher.by("time/nyt_length_20_train.txt")
    Config.resultsFileName = "train_ffn_time_decay.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val validation = (for {
      i <- 0 until 20
    } yield {Fetcher.by(s"time/nyt_length_20-${i}_validation.txt")}).reduce(_ ++ _)
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 5)
    Config.cats.foreach(c => trainNeuralNetwork(c, brinaryFFNTrainer, prefs, "ffn-time"))
  }

  def trainRNNSampled() = {
    val (train, validation) = Fetcher.ordered(true)
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 1000, epochs = 10)
    Config.cats.foreach(c => trainNeuralNetwork(c, binaryRNNTrainer, prefs, "rnn-sampled"))
  }

  def trainRNNOrdered() = {
    val (train, validation) = Fetcher.ordered(false)
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 1000, epochs = 10)
    Config.cats.foreach(c => trainNeuralNetwork(c, binaryRNNTrainer, prefs, "rnn-ordered"))
  }

  def trainRNNBalanced() = {
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 1000, epochs = 10)
      trainNeuralNetwork(c, binaryRNNTrainer, prefs, "rnn-balanced")
    })
  }

  def trainFFNConfidence() = {
    def train(confidence: Int): Array[Article] = Fetcher.by(s"confidence/nyt_mini_train_ordered_${confidence}.txt").collect
    def validation(confidence: Int): Array[Article] = Fetcher.by(s"confidence/nyt_mini_validation_ordered_${confidence}.txt").collect
    Config.resultsFileName = "train_ffn_confidence.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = null, validation = null, minibatchSize = 500, epochs = 5)
    Seq(25, 50, 75, 100).foreach(confidence => {
      Log.r(s"Training with confidence ${confidence} ...")
      Config.cats.foreach(c => trainNeuralNetwork(c, brinaryFFNTrainer, prefs.copy(train = train(confidence), validation = validation(confidence)), "ffa-confidence-" + confidence))
    })
  }

  def trainFFNOrdered(sub: Boolean = true) = {
    val (train, validation) = Fetcher.ordered(sub)
    Config.resultsFileName = "train_ffn_ordered.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 2)
    for {
      lr <- Seq(0.5)
      mbs <- Seq(500)
    } yield Config.cats.foreach(c => trainNeuralNetwork(c, brinaryFFNTrainer, prefs.copy(learningRate = lr, minibatchSize = mbs), "ffa-ordered"))
  }

  def trainFFNOrderedTypes(sub: Boolean = true) = {
    val (train, validation) = Fetcher.types(sub)
    Config.resultsFileName = "train_ffn_ordered_types.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 2)
    Config.cats.foreach(c => trainNeuralNetwork(c, brinaryFFNTrainer, prefs, "ffa-types"))
  }

  def trainFFNShuffled(sub: Boolean = true) = {
    val (train, validation) = Fetcher.shuffled(sub)
    Config.resultsFileName = "train_ffn_shuffled.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 2)
    Config.cats.foreach(c => trainNeuralNetwork(c, brinaryFFNTrainer, prefs, "ffa-shuffled"))
  }

  def trainFFNBalanced() = {
    Config.resultsFileName = "train_ffn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 1000, epochs = 10)
      trainNeuralNetwork(c, binaryRNNTrainer, prefs, "ffn-balanced")
    })
  }

  def trainFFNBoW() = {
    val (train, validation) = Fetcher.ordered(false)
    Config.resultsFileName = "train_ffn_bow.txt"
    Config.resultsCatsFileName = "train_ffn_bow_cats.txt"
    val phrases: Set[String] = Preprocess.computeTerms(train ++ validation, 100)
    for {
      learningRate <- Seq(0.05)
      minibatchSize = 1000
      c <- Config.cats.takeRight(1)
      histogram = false
      epochs = 1
    } yield {
      val neuralPrefs = NeuralPrefs(
        learningRate = learningRate, minibatchSize = minibatchSize, histogram = histogram, epochs = epochs,
        train = Preprocess.frequencyFilter(train, phrases),
        validation = Preprocess.frequencyFilter(validation, phrases)
      )
      Log.r(neuralPrefs)
      val net: MultiLayerNetwork = trainBinaryFFNBoW(c, neuralPrefs, phrases.toArray.sorted)
      NeuralModelLoader.save(net, c, Config.count, "bow")
      System.gc()
    }
  }

  def trainNaiveBayes(bow: Boolean, sub: Boolean = false) = {
    Config.resultsFileName = "train_nb.txt"
    Config.resultsCatsFileName = "train_nb_cats.txt"

    val (train, validation) = Fetcher.ordered(true)
    val prefs = sc.broadcast(Prefs())
    val phrases: Array[String] = (train ++ validation).flatMap(_.ann.keySet).collect.distinct.sorted
    Log.toFile(phrases, "nb_phrases.txt", Config.modelPath)
    val models = MlLibUtils.multiLabelClassification(prefs, train, validation, phrases, bow)
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"nb_${if (bow) "bow" else "w2v"}_${IPTC.trim(c)}.bin") }
  }
}

sealed trait NeuralTrainer {

  def trainNeuralNetwork(label: String, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, neuralPrefs: NeuralPrefs, name: String ) = {
    val net: MultiLayerNetwork = trainNetwork(label, neuralPrefs)
    NeuralModelLoader.save(net, label, Config.count, name)
    System.gc()
  }

  def sparkRNNTrainer(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    var net = RNN.createBinary(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val trainIter: List[DataSet] = new RNNIterator(neuralPrefs.train, Some(label), batchSize = neuralPrefs.minibatchSize).asScala.toList
    val testIter = new AsyncDataSetIterator(new RNNIterator(neuralPrefs.validation, Some(label), batchSize = neuralPrefs.minibatchSize))
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, 200, 2)
      val eval = NeuralEvaluation(net, testIter, i, label, Some(neuralPrefs))
      eval.log()
      testIter.reset()
    }

    net
  }

  def sparkFFNTrainer(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    var net = FeedForward.create(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val testIter: DataSetIterator = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize)
    val trainIter: List[DataSet] = new FeedForwardIterator(neuralPrefs.train, label, batchSize = neuralPrefs.minibatchSize).asScala.toList
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, neuralPrefs.minibatchSize * 8, 8)
      val eval = NeuralEvaluation(net, testIter, i, label, Some(neuralPrefs))
      eval.log()
      testIter.reset()
    }

    net
  }

  def binaryRNNTrainer(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = RNN.createBinary(neuralPrefs)
    val trainIter = new RNNIterator(neuralPrefs.train, Some(label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(neuralPrefs.validation, Some(label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  def brinaryFFNTrainer(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = FeedForward.create(neuralPrefs)
    val trainIter = new FeedForwardIterator(neuralPrefs.train, label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  def trainBinaryFFNBoW(label: String, neuralPrefs: NeuralPrefs, phrases: Array[String]): MultiLayerNetwork = {
    val net = FeedForward.createBoW(neuralPrefs, phrases.size)
    val trainIter = new FeedForwardIterator(neuralPrefs.train, label, batchSize = neuralPrefs.minibatchSize, phrases = phrases)
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize, phrases = phrases)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }
}


