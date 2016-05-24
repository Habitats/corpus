package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, _}
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralEvaluation, NeuralPrefs, NeuralTrainer}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
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
    trainFeedfowardW2V(train, validation, "subsampled-ffn-w2v", learningRate = 0.5)
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
    val (train, validation) = Fetcher.ordered
    trainRecurrentW2V(train, validation, "all-rnn-w2v")
  }

  def trainFFNW2V() = {
    val (train, validation) = Fetcher.ordered
    trainFeedfowardW2V(train, validation, "all-ffn-w2v")
  }

  def trainFFNBoW() = {
    val (train, validation) = Fetcher.ordered
    trainFeedforwardBoW(train, validation, "all-ffn-bow", termFrequencyThreshold = 100)
  }

  def trainNaiveW2V() = {
    val (train, validation) = Fetcher.ordered
    trainNaiveBayesW2V(train, validation, "all-nb-w2v")
  }

  def trainNaiveBoW() = {
    val (train, validation) = Fetcher.ordered
    trainNaiveBayesBoW(train, validation, "all-nb-bow", termFrequencyThreshold = 100)
  }

  // Ex2 - Confidence
  def trainFFNConfidence() = {
    def train(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_train_ordered_${confidence}.txt")
    def validation(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_validation_ordered_${confidence}.txt")
    Seq(25, 50, 75, 100).foreach(confidence => {
      Log.r(s"Training with confidence ${confidence} ...")
      trainFeedfowardW2V(train = train(confidence), validation = validation(confidence), name = "ffa-confidence-" + confidence)
    })
  }

  // Ex3 - Types
  def trainFFNW2VTypes() = {
    val (train, validation) = Fetcher.types
    trainFeedfowardW2V(train, validation, "ffa-w2v-types")
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
    trainFeedfowardW2V(train, validation, "time-ffn-w2v", learningRate = 0.5)
  }

  // Misc
  def trainFFNShuffled(sub: Boolean = true) = {
    val (train, validation) = Fetcher.shuffled
    trainFeedforwardBoW(train, validation, "ffn-shuffled")
  }

  def trainRNNBalanced() = {
    Config.resultsFileName = "train_rnn_bow_balanced.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val validation = Fetcher.annotatedValidationOrdered
    Config.cats.foreach(c => {
      val train = Fetcher.by(s"balanced/nyt_${IPTC.trim(c)}_superbalanced.txt")
      ???
    })
  }

  def trainNaiveBalanced() = {
    Config.resultsFileName = "train_nb_bow_balanced.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val validation = Fetcher.annotatedValidationOrdered
    Config.cats.foreach(c => {
      val train = Fetcher.by(s"balanced/nyt_${IPTC.trim(c)}_superbalanced.txt")
      ???
    })
  }

  def trainFFNBalanced() = {
    Config.resultsFileName = "train_ffn_w2v_balanced.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val validation = Fetcher.annotatedValidationOrdered
    Config.cats.foreach(c => {
      val train = Fetcher.by(s"balanced/nyt_${IPTC.trim(c)}_superbalanced.txt")
      ???
    })
  }

  // SPARK
  def trainFFNSparkOrdered() = {
    val (train, validation) = Fetcher.ordered
    trainSpark(train, validation, "ffn-w2v-spark", sparkFFNTrainer)
  }

  def trainRNNW2VSpark() = {
    val (train, validation) = Fetcher.ordered
    trainSpark(train, validation, "rnn-w2v-spark", sparkRNNTrainer)
  }
}

sealed trait NeuralTrainer {

  import scala.collection.JavaConverters._

  implicit def collect(rdd: RDD[Article]): Array[Article] = rdd.collect()

  private val count: String = if (Config.count == Int.MaxValue) "all" else Config.count.toString

  def trainFeedfowardW2V(train: RDD[Article], validation: RDD[Article], name: String, learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)), minibatchSize: Int = Config.miniBatchSize.getOrElse(1000)) = {
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    for {lr <- learningRate} yield {
      val prefs = NeuralPrefs(learningRate = lr, train = train, validation = validation, minibatchSize = minibatchSize, epochs = 1)
      if (Config.spark) {
        sc.parallelize(Config.cats).map(c => (c, brinaryFFNW2VTrainer(c, prefs))).foreach(n => NeuralModelLoader.save(n._2, n._1, Config.count, name + "-" + count))
      } else {
        Config.cats.foreach(c => trainNeuralNetwork(c, brinaryFFNW2VTrainer, prefs, name))
      }
    }
  }

  def trainRecurrentW2V(train: RDD[Article], validation: RDD[Article], name: String, learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)), minibatchSize: Int = Config.miniBatchSize.getOrElse(500), hiddenNodes: Int = Config.hidden.getOrElse(100)): Seq[Unit] = {
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    for {lr <- learningRate} yield {
      val prefs = NeuralPrefs(learningRate = lr, train = train, validation = validation, minibatchSize = minibatchSize, epochs = 1, hiddenNodes = hiddenNodes)
      Config.cats.foreach(c => trainNeuralNetwork(c, binaryRNNTrainer, prefs, name))
    }
  }

  def trainFeedforwardBoW(train: RDD[Article], validation: RDD[Article], name: String, learningRate: Double = Config.learningRate.getOrElse(0.05), minibatchSize: Int = Config.miniBatchSize.getOrElse(1000), termFrequencyThreshold: Int = 100) = {
    Config.resultsFileName = s"train_$name.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "-" + count + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    Config.cats.zipWithIndex.foreach { case (c, i) => {
      val neuralPrefs = NeuralPrefs(
        learningRate = learningRate, minibatchSize = minibatchSize, epochs = 1,
        train = TFIDF.frequencyFilter(train, tfidf.phrases),
        validation = TFIDF.frequencyFilter(validation, tfidf.phrases))
      val net: MultiLayerNetwork = binaryFFNBoWTrainer(c, neuralPrefs, tfidf)
      NeuralModelLoader.save(net, c, Config.count, s"$name-$count")
      System.gc()
    }
    }
  }

  def trainNaiveBayesW2V(train: RDD[Article], validation: RDD[Article], name: String) = {
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    trainNaiveBayes(train, validation, name, None)
  }

  def trainNaiveBayesBoW(train: RDD[Article], validation: RDD[Article], name: String, termFrequencyThreshold: Int) = {
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "-" + count + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    trainNaiveBayes(TFIDF.frequencyFilter(train, tfidf.phrases), TFIDF.frequencyFilter(validation, tfidf.phrases), name, Some(tfidf))
  }

  private def trainNaiveBayes(train: RDD[Article], validation: RDD[Article], name: String, tfidf: Option[TFIDF] = None) = {
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val prefs = sc.broadcast(Prefs())
    val models = MlLibUtils.multiLabelClassification(prefs, train, validation, tfidf)
    val fullName = name + (if (Config.count != Int.MaxValue) s"_$count" else "")
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"$fullName/${name}_${IPTC.trim(c)}.bin") }
  }

  private def trainNeuralNetwork(label: String, trainNetwork: (String, NeuralPrefs) => MultiLayerNetwork, neuralPrefs: NeuralPrefs, name: String) = {
    val net: MultiLayerNetwork = trainNetwork(label, neuralPrefs)
    NeuralModelLoader.save(net, label, Config.count, name + "-" + count)
    System.gc()
  }

  def trainSpark(train: RDD[Article], validation: RDD[Article], name: String, trainer: (String, NeuralPrefs) => MultiLayerNetwork) = {
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    for {
      lr <- Seq(0.075, 0.1, 0.2, 0.3, 0.4)
      mbs <- Seq(1000, 2000, 3000)
    } yield {
      val prefs = NeuralPrefs(learningRate = lr, train = train, validation = validation, minibatchSize = mbs, epochs = 1)
      Config.cats.foreach(c => trainNeuralNetwork(c, trainer, prefs, "ffn-spark"))
    }
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
      val eval = NeuralEvaluation(net, testIter.asScala, i, label, Some(neuralPrefs))
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
      val eval = NeuralEvaluation(net, testIter.asScala, i, label, Some(neuralPrefs))
      eval.log()
      testIter.reset()
    }

    net
  }

  private def binaryRNNTrainer(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = RNN.createBinary(neuralPrefs)
    val trainIter = new RNNIterator(neuralPrefs.train, Some(label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(neuralPrefs.validation, Some(label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  private def brinaryFFNW2VTrainer(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = FeedForward.create(neuralPrefs)
    val trainIter = new FeedForwardIterator(neuralPrefs.train, label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  private def binaryFFNBoWTrainer(label: String, neuralPrefs: NeuralPrefs, tfidf: TFIDF): MultiLayerNetwork = {
    val net = FeedForward.createBoW(neuralPrefs, tfidf.phrases.size)
    val trainIter = new FeedForwardIterator(neuralPrefs.train, label, batchSize = neuralPrefs.minibatchSize, Some(tfidf))
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize, Some(tfidf))
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }
}


