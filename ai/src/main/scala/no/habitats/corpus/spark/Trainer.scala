package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, _}
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralPrefs, NeuralTrainer}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.language.implicitConversions

object Trainer extends Serializable {
  implicit def seqthis(a: Double): Seq[Double] = Seq(a)

  // ### Best models
  // ## Subsampled
  def trainRNNSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    RecurrentTrainer(tag = Some("sub"), learningRate = 0.5).trainW2V(train, validation)
  }

  def trainFFNW2VSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    FeedforwardTrainer(tag = Some("sub"), learningRate = 0.5).trainW2V(train, validation)
  }

  def trainFFNBoWSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    FeedforwardTrainer(tag = Some("sub"), learningRate = 0.5).trainBoW(train, validation, termFrequencyThreshold = 5)
  }

  def trainNaiveW2VSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    NaiveBayesTrainer(tag = Some("sub")).trainW2V(train, validation)
  }

  def trainNaiveBoWSubsampled() = {
    val (train, validation) = Fetcher.subsampled
    NaiveBayesTrainer(tag = Some("sub")).trainBoW(train, validation, termFrequencyThreshold = 5)
  }

  // Chosen baseline
  def trainRNNW2V() = {
    val (train, validation) = Fetcher.ordered()
    RecurrentTrainer().trainW2V(train, validation)
  }

  def trainFFNW2V() = {
    val (train, validation) = Fetcher.ordered()
    FeedforwardTrainer().trainW2V(train, validation)
  }

  def trainFFNBoW() = {
    val (train, validation) = Fetcher.ordered()
    FeedforwardTrainer().trainBoW(train, validation, termFrequencyThreshold = 100)
  }

  def trainNaiveW2V() = {
    val (train, validation) = Fetcher.ordered()
    NaiveBayesTrainer().trainW2V(train, validation)
  }

  def trainNaiveBoW() = {
    val (train, validation) = Fetcher.ordered()
    NaiveBayesTrainer().trainBoW(train, validation, termFrequencyThreshold = 100)
  }

  // Ex2 - Confidence
  def trainFFNConfidence() = {
    def train(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_train_ordered_${confidence}.txt")
    def validation(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_validation_ordered_${confidence}.txt")
    Seq(25, 50, 75, 100).foreach(confidence => {
      Log.r(s"Training with confidence ${confidence} ...")
      val tag: Some[String] = Some(s"confidence-$confidence")
      FeedforwardTrainer(tag = tag).trainW2V(train = train(confidence), validation = validation(confidence))
      FeedforwardTrainer(tag = tag).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 100)
      NaiveBayesTrainer(tag = tag).trainW2V(train = train(confidence), validation = validation(confidence))
      NaiveBayesTrainer(tag = tag).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 100)
    })
  }

  // Ex3 - Types
  def trainFFNW2VTypes() = {
    val (train, validation) = Fetcher.ordered(types = true)
    FeedforwardTrainer(tag = Some("types")).trainW2V(train, validation)
  }

  def trainRNNW2VTypes() = {
    val (train, validation) = Fetcher.ordered(types = true)
    RecurrentTrainer(tag = Some("types")).trainW2V(train, validation)
  }

  // Ex4 - Lenghts - Use baseline

  // Ex5 - Extrapolation
  def trainFFNBoWTime() = {
    val train = Fetcher.by("time/nyt_time_train.txt")
    val validation = Fetcher.by("time/nyt_time_0_validation.txt")
    FeedforwardTrainer(tag = Some("time")).trainBoW(train, validation)
  }

  def trainFFNW2VTime() = {
    val train = Fetcher.by("time/nyt_time_train.txt")
    val validation = Fetcher.by("time/nyt_time_0_validation.txt")
    FeedforwardTrainer(tag = Some("time"), learningRate = 0.5).trainW2V(train, validation)
  }
}

sealed trait ModelTrainer {

  val count: String = if (Config.count == Int.MaxValue) "all" else Config.count.toString
  val prefix     : String
  val tag        : Option[String]
  val superSample: Boolean

  lazy val name: String = s"${tag.map(_ + "_").getOrElse("")}${prefix}${if (superSample) "_super" else ""}${if (Config.count == Int.MaxValue) "_all" else "_" + Config.count}"

  def init(types: Boolean): Unit = {
    Config.resultsFileName = s"train_${name}.txt"
    Config.resultsCatsFileName = Config.resultsFileName
  }

  def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int = 100)

  def trainW2V(train: RDD[Article], validation: RDD[Article])

  def processTraining(train: RDD[Article], superSample: Boolean): (String => RDD[Article]) = {
    label => if (superSample) Cacher.supersampledBalanced(label, train) else train
  }
}

sealed trait NeuralTrainer {

  def trainNetwork(validation: RDD[Article], training: (String) => RDD[Article], name: String, minibatchSize: Int, learningRate: Seq[Double], trainer: (String, NeuralPrefs, RDD[Article]) => MultiLayerNetwork) = {
    for {lr <- learningRate} yield {
      val prefs = NeuralPrefs(learningRate = lr, validation = validation.collect(), minibatchSize = minibatchSize, epochs = 1)
      Config.cats.foreach(c => {
        val processedTraining: RDD[Article] = training(c)
        val net: MultiLayerNetwork = trainer(c, prefs, processedTraining)
        NeuralModelLoader.save(net, c, Config.count, name)
        System.gc()
      })
    }
  }

}

sealed case class FeedforwardTrainer(
                                      learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)),
                                      minibatchSize: Int = Config.miniBatchSize.getOrElse(1000),
                                      superSample: Boolean = Config.superSample.getOrElse(false),
                                      tag: Option[String] = None
                                    ) extends ModelTrainer with NeuralTrainer {

  override val prefix = "nb"

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    trainNetwork(validation, processTraining(train, superSample), name, minibatchSize, learningRate, binaryFFNW2VTrainer)
  }

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int = 100) = {
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    val processedValidation: RDD[Article] = TFIDF.frequencyFilter(validation, tfidf.phrases)
    val processedTraining: RDD[Article] = TFIDF.frequencyFilter(train, tfidf.phrases)
    trainNetwork(processedValidation, processTraining(processedTraining, superSample), name, minibatchSize, learningRate, (label, neuralPrefs, train) => binaryFFNBoWTrainer(label, neuralPrefs, train, tfidf))
  }

  private def binaryFFNW2VTrainer(label: String, neuralPrefs: NeuralPrefs, train: RDD[Article]): MultiLayerNetwork = {
    val net = FeedForward.create(neuralPrefs)
    val trainIter = new FeedForwardIterator(train.collect(), label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }

  private def binaryFFNBoWTrainer(label: String, neuralPrefs: NeuralPrefs, train: RDD[Article], tfidf: TFIDF): MultiLayerNetwork = {
    val net = FeedForward.createBoW(neuralPrefs, tfidf.phrases.size)
    val trainIter = new FeedForwardIterator(train.collect(), label, batchSize = neuralPrefs.minibatchSize, Some(tfidf))
    val testIter = new FeedForwardIterator(neuralPrefs.validation, label, batchSize = neuralPrefs.minibatchSize, Some(tfidf))
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }
}

sealed case class RecurrentTrainer(
                                    learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)),
                                    minibatchSize: Int = Config.miniBatchSize.getOrElse(1000),
                                    superSample: Boolean = Config.superSample.getOrElse(false),
                                    hiddenNodes: Int = Config.hidden1.getOrElse(10),
                                    tag: Option[String] = None) extends ModelTrainer with NeuralTrainer {

  override val prefix = "nb"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = ???

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    trainNetwork(validation, processTraining(train, superSample), name, minibatchSize, learningRate, binaryRNNTrainer)
  }

  // Binary trainers
  private def binaryRNNTrainer(label: String, neuralPrefs: NeuralPrefs, train: RDD[Article]): MultiLayerNetwork = {
    val net = RNN.createBinary(neuralPrefs.copy(hiddenNodes = hiddenNodes))
    val trainIter = new RNNIterator(train.collect(), Some(label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(neuralPrefs.validation, Some(label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(label, neuralPrefs, net, trainIter, testIter)
  }
}

sealed case class NaiveBayesTrainer(superSample: Boolean = false, tag: Option[String] = None) extends ModelTrainer {

  override val prefix = "nb"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "-" + count + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    trainNaiveBayes(TFIDF.frequencyFilter(train, tfidf.phrases), TFIDF.frequencyFilter(validation, tfidf.phrases), Some(tfidf), superSample)
  }

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    trainNaiveBayes(train, validation, None, superSample)
  }

  private def trainNaiveBayes(train: RDD[Article], validation: RDD[Article], tfidf: Option[TFIDF] = None, superSample: Boolean) = {
    Config.resultsFileName = s"train_$name.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    val models: Map[String, NaiveBayesModel] = Config.cats.map(c => (c, MlLibUtils.multiLabelClassification(c, processTraining(train, superSample)(c), validation, tfidf))).toMap
    val predicted = MlLibUtils.testMLlibModels(validation, models, tfidf)
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs()))
    val fullName = name + (if (Config.count != Int.MaxValue) s"_$count" else "")
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"$fullName/${name}_${IPTC.trim(c)}.bin") }
  }
}
