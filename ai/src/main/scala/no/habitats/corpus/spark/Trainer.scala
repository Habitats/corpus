package no.habitats.corpus.spark

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

  implicit def collect(rdd: RDD[Article]): Array[Article] = rdd.collect()

  def trainRNNSpark() = {
    val (train, validation) = Fetcher.ordered(true)
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(train = train, validation = validation)
    trainSparkRNN("sport", prefs)
  }

  def trainFFNSpark() = {
    val (train, validation) = Fetcher.ordered(true)
    Config.resultsFileName = "train_ffn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(train = train, validation = validation)
    trainSparkFFN("sport", prefs)
  }

  def trainRNNSampled() = {
    val (train, validation) = Fetcher.ordered(true)
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryRNN, train, validation, "sampled"))
  }

  def trainRNNBalanced() = {
    Config.resultsFileName = "train_rnn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, trainBinaryRNN, train, validation, "sampled")
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
      Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, prefs.copy(train = train(confidence), validation = validation(confidence)), confidence.toString))
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
    } yield Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, prefs.copy(learningRate = lr, minibatchSize = mbs)))
  }

  def trainFFNOrderedTypes(sub: Boolean = true) = {
    val (train, validation) = Fetcher.types(sub)
    Config.resultsFileName = "train_ffn_ordered_types.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 2)
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, prefs))
  }

  def trainFFNShuffled(sub: Boolean = true) = {
    val (train, validation) = Fetcher.shuffled(sub)
    Config.resultsFileName = "train_ffn_shuffled.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 2)
    Config.cats.foreach(c => trainNeuralNetwork(c, trainBinaryFFN, prefs))
  }

  def trainFFNBalanced() = {
    Config.resultsFileName = "train_ffn.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    Config.cats.foreach(c => {
      val train = Fetcher.balanced(IPTC.trim(c) + "_train", true)
      val validation = Fetcher.balanced(IPTC.trim(c) + "_validation", false)
      trainNeuralNetwork(c, trainBinaryRNN, train, validation, "balanced")
    })
  }

  def train(name: String, train: RDD[Article], validation: RDD[Article], spark: Boolean = false) = {
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    if (name.startsWith("ffn")) {
      val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 500, epochs = 1)
      Config.cats.foreach(c => trainNeuralNetwork(c, if (!spark) trainBinaryFFN else trainSparkFFN, prefs))
    } else if (name.startsWith("rnn")) {
      val prefs = NeuralPrefs(learningRate = 0.05, train = train, validation = validation, minibatchSize = 100, epochs = 1, hiddenNodes = 20)
      Config.cats.foreach(c => trainNeuralNetwork(c, if (!spark) trainBinaryRNN else trainSparkRNN, prefs))
    }
  }

  def trainAll() = {
    train("ffn_ordered", Fetcher.annotatedTrainOrdered, Fetcher.annotatedValidationOrdered)
    train("ffn_shuffled", Fetcher.annotatedTrainShuffled, Fetcher.annotatedValidationShuffled)

    train("ffn_sub_ordered", Fetcher.subTrainOrdered, Fetcher.subValidationOrdered)
    train("ffn_sub_shuffled", Fetcher.subTrainShuffled, Fetcher.subValidationShuffled)
    train("ffn_sub_ordered_types", Fetcher.subTrainOrderedTypes, Fetcher.subValidationOrdered)

    train("rnn_sub_ordered", Fetcher.subTrainOrdered, Fetcher.subValidationOrdered)
  }

  def trainSparkRNN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
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

  def trainSparkFFN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    var net = FeedForward.create(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val testIter: DataSetIterator = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize)
    val trainIter: List[DataSet] = new FeedForwardIterator(neuralPrefs.train, label, batchSize = neuralPrefs.minibatchSize).asScala.toList
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

  def trainBinaryRNN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = RNN.createBinary(neuralPrefs)
    val trainIter = new RNNIterator(neuralPrefs.train, Some(label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(neuralPrefs.validation, Some(label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  def trainBinaryFFN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
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

  def trainNeuralNetwork(c: String, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, train: RDD[Article], validation: RDD[Article], name: String): Unit = {
    for {
      hiddenNodes <- Seq(300)
      learningRate <- Seq(0.05)
      minibatchSize = 100
      //            c <- cats
      histogram = false
      epochs = 1
    } yield {
      val neuralPrefs = NeuralPrefs(learningRate = learningRate, hiddenNodes = hiddenNodes, train = train, validation = validation, minibatchSize = minibatchSize, histogram = histogram, epochs = epochs)
      trainNeuralNetwork(c, trainNetwork, neuralPrefs, name)
    }
  }

  def trainNeuralNetwork(c: String, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, neuralPrefs: NeuralPrefs, name: String = "") = {
    val net: MultiLayerNetwork = trainNetwork(c, neuralPrefs)
    NeuralModelLoader.save(net, name + "_" + c + "_" + (System.currentTimeMillis() / 1000).toString.substring(5), Config.count)
    System.gc()
  }

  def trainFFNSubSampledBoW() = {
    val (train, validation) = Fetcher.ordered(true)
    Config.resultsFileName = "train_ffn_bow.txt"
    Config.resultsCatsFileName = "train_ffn_bow_cats.txt"
    val phrases: Array[String] = (train ++ validation).flatMap(_.ann.keySet).collect.distinct.sorted

    for {
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


