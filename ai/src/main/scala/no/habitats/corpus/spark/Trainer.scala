package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, _}
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralEvaluation, NeuralPrefs, NeuralTrainer}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.{AsyncDataSetIterator, DataSetIterator}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.nd4j.linalg.dataset.DataSet

import scala.language.implicitConversions

object Trainer extends NeuralTrainer with Serializable {
  implicit def seqthis(a: Double): Seq[Double] = Seq(a)

  // ### Best models
  // ## Subsampled
  def trainRNNSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    trainRecurrentW2V(train, validation, "subsampled-rnn-w2v", learningRate = 0.5)
  }

  def trainFFNW2VSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    trainFeedforwardW2V(train, validation, "subsampled-ffn-w2v", learningRate = 0.5)
  }

  def trainFFNBoWSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    trainFeedforwardBoW(train, validation, "subsampled-ffn-bow", learningRate = 0.5, termFrequencyThreshold = 5)
  }

  def trainNaiveW2VSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    trainNaiveBayesW2V(train, validation, "subsampled-nb-w2v")
  }

  def trainNaiveBoWSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    trainNaiveBayesBoW(train, validation, "subsampled-nb-bow", termFrequencyThreshold = 5)
  }

  // Chosen baseline
  def trainRNNW2V() = {
    val (train, validation) = Fetcher.ordered()
    trainRecurrentW2V(train, validation, "all-rnn-w2v")
  }

  def trainFFNW2V() = {
    val (train, validation) = Fetcher.ordered()
    trainFeedforwardW2V(train, validation, "all-ffn-w2v")
  }

  def trainFFNBoW() = {
    val (train, validation) = Fetcher.ordered()
    trainFeedforwardBoW(train, validation, "all-ffn-bow", termFrequencyThreshold = 100)
  }

  def trainNaiveW2V() = {
    val (train, validation) = Fetcher.ordered()
    trainNaiveBayesW2V(train, validation, "all-nb-w2v")
  }

  def trainNaiveBoW() = {
    val (train, validation) = Fetcher.ordered()
    trainNaiveBayesBoW(train, validation, "all-nb-bow", termFrequencyThreshold = 100)
  }

  // Ex2 - Confidence
  def trainFFNConfidence() = {
    def train(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_train_ordered_${confidence}.txt")
    def validation(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_validation_ordered_${confidence}.txt")
    Seq(25, 50, 75, 100).foreach(confidence => {
      Log.r(s"Training with confidence ${confidence} ...")
      trainFeedforwardW2V(train = train(confidence), validation = validation(confidence), name = "ffa-w2v-confidence-" + confidence)
      trainFeedforwardBoW(train = train(confidence), validation = validation(confidence), name = "ffa-bow-confidence-" + confidence, termFrequencyThreshold = 10)
      trainNaiveBayesW2V(train = train(confidence), validation = validation(confidence), name = "nb-w2v-confidence-" + confidence)
      trainNaiveBayesBoW(train = train(confidence), validation = validation(confidence), name = "nb-bow-confidence-" + confidence, termFrequencyThreshold = 10)
    })
  }

  // Ex3 - Types
  def trainFFNW2VTypes() = {
    val (train, validation) = Fetcher.ordered(types = true)
    trainFeedforwardW2V(train, validation, "types-ffa-w2v")
  }

  def trainRNNW2VTypes() = {
    val (train, validation) = Fetcher.ordered(types = true)
    trainRecurrentW2V(train, validation, "types-rrn-w2v")
  }

  // Ex4 - Lenghts - Use baseline

  // Ex5 - Extrapolation
  def trainFFNBoWTime() = {
    val train = Fetcher.by("time/nyt_time_train.txt")
    val validation = Fetcher.by("time/nyt_time_0_validation.txt")
    trainFeedforwardBoW(train, validation, "time-ffn-bow", termFrequencyThreshold = 5, learningRate = 0.5)
  }

  def trainFFNW2VTime() = {
    val train = Fetcher.by("time/nyt_time_train.txt")
    val validation = Fetcher.by("time/nyt_time_0_validation.txt")
    trainFeedforwardW2V(train, validation, "time-ffn-w2v", learningRate = 0.5)
  }

  // SPARK
  def trainFFNSparkOrdered() = {
    val (train, validation) = Fetcher.ordered()
    trainSpark(train, validation, "ffn-w2v-spark", sparkFFNTrainer)
  }

  def trainRNNW2VSpark() = {
    val (train, validation) = Fetcher.ordered()
    trainSpark(train, validation, "rnn-w2v-spark", sparkRNNTrainer)
  }
}

sealed trait NeuralTrainer {

  import scala.collection.JavaConverters._

  implicit def collect(rdd: RDD[Article]): Array[Article] = rdd.collect()

  private val count: String = if (Config.count == Int.MaxValue) "all" else Config.count.toString

  def trainFeedforwardW2V(train: RDD[Article], validation: RDD[Article], name: String, learningRate: Seq[Double] = Seq(0.05), minibatchSize: Int = 1000, superSample: Boolean = false) = {
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    for {lr <- learningRate} yield {
      val prefs = NeuralPrefs(learningRate = lr, validation = validation, minibatchSize = minibatchSize, epochs = 1)
      Config.cats.foreach(c => trainNeuralNetwork(c, binaryFFNW2VTrainer, prefs, name, processTraining(train, c, superSample)))
    }
  }

  def processTraining(train: RDD[Article], label: String, superSample: Boolean): RDD[Article] = {
    if (Config.superSample.getOrElse(superSample)) Cacher.supersampledBalanced(label, train) else train
  }

  def trainRecurrentW2V(train: RDD[Article], validation: RDD[Article], name: String, learningRate: Seq[Double] = Seq(0.05), minibatchSize: Int = 500, hiddenNodes: Int = 100, superSample: Boolean = false): Seq[Unit] = {
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    for {lr <- learningRate} yield {
      val prefs = NeuralPrefs(learningRate = lr, validation = validation, minibatchSize = minibatchSize, epochs = 1, hiddenNodes = hiddenNodes)
      Config.cats.foreach(c => trainNeuralNetwork(c, binaryRNNTrainer, prefs, name, processTraining(train, c, superSample)))
    }
  }

  def trainFeedforwardBoW(train: RDD[Article], validation: RDD[Article], name: String, learningRate: Double = 0.05, minibatchSize: Int = 1000, termFrequencyThreshold: Int = 100, superSample: Boolean = false) = {
    Config.resultsFileName = s"train_$name.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    Config.cats.zipWithIndex.foreach { case (c, i) => {
      val bowTraining: RDD[Article] = TFIDF.frequencyFilter(train, tfidf.phrases)
      val neuralPrefs = NeuralPrefs(
        learningRate = learningRate, minibatchSize = minibatchSize, epochs = 1,
        validation = TFIDF.frequencyFilter(validation, tfidf.phrases))
      val net: MultiLayerNetwork = binaryFFNBoWTrainer(c, neuralPrefs, processTraining(train, c, superSample), tfidf)
      NeuralModelLoader.save(net, c, Config.count, s"$name-$count")
      System.gc()
    }
    }
  }

  def trainNaiveBayesW2V(train: RDD[Article], validation: RDD[Article], name: String, superSample: Boolean = false) = {
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    trainNaiveBayes(train, validation, name, None, superSample)
  }

  def trainNaiveBayesBoW(train: RDD[Article], validation: RDD[Article], name: String, termFrequencyThreshold: Int, superSample: Boolean = false) = {
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "-" + count + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    trainNaiveBayes(TFIDF.frequencyFilter(train, tfidf.phrases), TFIDF.frequencyFilter(validation, tfidf.phrases), name, Some(tfidf), superSample)
  }

  def trainNaiveBayes(train: RDD[Article], validation: RDD[Article], name: String, tfidf: Option[TFIDF] = None, superSample: Boolean) = {
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val models: Map[String, NaiveBayesModel] = Config.cats.map(c => (c, MlLibUtils.multiLabelClassification(c, processTraining(train, c, superSample), validation, tfidf))).toMap
    val predicted = MlLibUtils.testMLlibModels(validation, models, tfidf)
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs()))
    val fullName = name + (if (Config.count != Int.MaxValue) s"_$count" else "")
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"$fullName/${name}_${IPTC.trim(c)}.bin") }
  }

  private def trainNeuralNetwork(label: String, trainNetwork: (String, NeuralPrefs, Array[Article]) => MultiLayerNetwork, neuralPrefs: NeuralPrefs, name: String, train: Array[Article]) = {
    val net: MultiLayerNetwork = trainNetwork(label, neuralPrefs, train)
    NeuralModelLoader.save(net, label, Config.count, name + "-" + count)
    System.gc()
  }

  def trainSpark(train: RDD[Article], validation: RDD[Article], name: String, trainer: (String, NeuralPrefs, Array[Article]) => MultiLayerNetwork) = {
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    for {
      lr <- Seq(0.075, 0.1, 0.2, 0.3, 0.4)
      mbs <- Seq(1000, 2000, 3000)
    } yield {
      val prefs = NeuralPrefs(learningRate = lr, validation = validation, minibatchSize = mbs, epochs = 1)
      Config.cats.foreach(c => trainNeuralNetwork(c, trainer, prefs, "ffn-spark", train))
    }
  }

  def sparkRNNTrainer(label: String, neuralPrefs: NeuralPrefs, train: Array[Article]): MultiLayerNetwork = {
    var net = RNN.createBinary(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val trainIter: List[DataSet] = new RNNIterator(train, Some(label), batchSize = neuralPrefs.minibatchSize).asScala.toList
    val testIter = new AsyncDataSetIterator(new RNNIterator(neuralPrefs.validation, Some(label), batchSize = neuralPrefs.minibatchSize))
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, 200, 2)
      val eval = NeuralEvaluation(net, testIter.asScala, i, label, Some(neuralPrefs))
      eval.log()
      testIter.reset()
    }

    net
  }

  def sparkFFNTrainer(label: String, neuralPrefs: NeuralPrefs, train: Array[Article]): MultiLayerNetwork = {
    var net = FeedForward.create(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val testIter: DataSetIterator = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize)
    val trainIter: List[DataSet] = new FeedForwardIterator(train, label, batchSize = neuralPrefs.minibatchSize).asScala.toList
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, neuralPrefs.minibatchSize * 8, 8)
      val eval = NeuralEvaluation(net, testIter.asScala, i, label, Some(neuralPrefs))
      eval.log()
      testIter.reset()
    }

    net
  }

  private def binaryRNNTrainer(label: String, neuralPrefs: NeuralPrefs, train: Array[Article]): MultiLayerNetwork = {
    val net = RNN.createBinary(neuralPrefs)
    val trainIter = new RNNIterator(train, Some(label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(neuralPrefs.validation, Some(label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  private def binaryFFNW2VTrainer(label: String, neuralPrefs: NeuralPrefs, train: Array[Article]): MultiLayerNetwork = {
    val net = FeedForward.create(neuralPrefs)
    val trainIter = new FeedForwardIterator(train, label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  private def binaryFFNBoWTrainer(label: String, neuralPrefs: NeuralPrefs, train: Array[Article], tfidf: TFIDF): MultiLayerNetwork = {
    val net = FeedForward.createBoW(neuralPrefs, tfidf.phrases.size)
    val trainIter = new FeedForwardIterator(train, label, batchSize = neuralPrefs.minibatchSize, Some(tfidf))
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize, Some(tfidf))
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }
}


