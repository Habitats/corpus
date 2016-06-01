package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.{Article, CorpusDataset}
import no.habitats.corpus.common.{Config, _}
import no.habitats.corpus.dl4j.NeuralTrainer.{IteratorPrefs, NeuralResult}
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralPrefs, NeuralTrainer}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.language.implicitConversions

object Trainer extends Serializable {

  implicit def seqthis(a: Double): Seq[Double] = Seq(a)

  // Ex1/2 - Baseline
  def baseline() = {
    val (train, validation) = Fetcher.ordered()
    val tag = "baseline"
    val learningRate = 0.05
    FeedforwardTrainer(tag, learningRate).trainW2V(train, validation)
    FeedforwardTrainer(tag, learningRate).trainBoW(train, validation, termFrequencyThreshold = 100)
    //    NaiveBayesTrainer(tag).trainW2V(train, validation)
    //    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 100)
    RecurrentTrainer(tag, learningRate).trainW2V(train, validation)
  }

  def supersampled() = {
    val (train, validation) = Fetcher.ordered()
    val tag = "super"
    val learningRate = 0.05
    FeedforwardTrainer(tag, learningRate, superSample = true).trainW2V(train, validation)
    FeedforwardTrainer(tag, learningRate, superSample = true).trainBoW(train, validation, termFrequencyThreshold = 100)
    NaiveBayesTrainer(tag, superSample = true).trainW2V(train, validation)
    NaiveBayesTrainer(tag, superSample = true).trainBoW(train, validation, termFrequencyThreshold = 100)
    RecurrentTrainer(tag, learningRate, superSample = true).trainW2V(train, validation)
  }

  // Ex3 - Types
  def types() = {
    val (train, validation) = Fetcher.ordered(types = true)
    val tag = "types"
    val learningRate = 0.05
//    FeedforwardTrainer(tag, learningRate).trainW2V(train, validation)
//    FeedforwardTrainer(tag, learningRate).trainBoW(train, validation, termFrequencyThreshold = 100)
    NaiveBayesTrainer(tag).trainW2V(train, validation)
    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 100)
//    RecurrentTrainer(tag, learningRate).trainW2V(train, validation)
  }

  // Ex5 - Time
  def time() = {
    val train = Fetcher.by("time/nyt_time_10_train.txt")
    val validation = Fetcher.by("time/nyt_time_10-0_validation.txt")
    val tag = "time"
    val learningRate = 0.5
    //    FeedforwardTrainer(tag, learningRate).trainW2V(train, validation)
    //    FeedforwardTrainer(tag, learningRate).trainBoW(train, validation, termFrequencyThreshold = 20)
    NaiveBayesTrainer(tag).trainW2V(train, validation)
    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 20)
    RecurrentTrainer(tag, learningRate).trainW2V(train, validation)
  }

  // Ex2 - Confidence
  def confidence() = {
    def train(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_train_ordered_${confidence}.txt")
    def validation(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_validation_ordered_${confidence}.txt")
    def tag(confidence: Int): String = s"confidence-$confidence"

    val learningRates = Seq(0.5)
    var confidence = 25
    val termFrequencyThreshold: Int = 10
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //        FeedforwardTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence), learningRates).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    RecurrentTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 50
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    FeedforwardTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence), learningRates).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    RecurrentTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 75
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    FeedforwardTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence), learningRates).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    RecurrentTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 100
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    FeedforwardTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence), learningRates).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    RecurrentTrainer(tag(confidence), learningRates).trainW2V(train = train(confidence), validation = validation(confidence))
  }

  // ### Best models
  // Chosen baseline
  def trainRNNW2V() = {
    val (train, validation) = Fetcher.ordered()
    RecurrentTrainer(Config.tag.getOrElse(Config.prefsName), Seq(Config.learningRate.getOrElse(0.05)), superSample = Config.superSample.getOrElse(false)).trainW2V(train, validation)
  }

  def trainFFNW2V() = {
    val (train, validation) = Fetcher.ordered()
    FeedforwardTrainer(Config.tag.getOrElse(Config.prefsName), Seq(Config.learningRate.getOrElse(0.05)), superSample = Config.superSample.getOrElse(false)).trainW2V(train, validation)
  }

  def trainFFNBoW() = {
    val (train, validation) = Fetcher.ordered()
    FeedforwardTrainer(Config.tag.getOrElse(Config.prefsName), Seq(Config.learningRate.getOrElse(0.05)), superSample = Config.superSample.getOrElse(false)).trainBoW(train, validation, termFrequencyThreshold = 100)
  }

  def trainNaiveW2V() = {
    val (train, validation) = Fetcher.ordered()
    NaiveBayesTrainer(Config.tag.getOrElse(Config.prefsName), superSample = Config.superSample.getOrElse(false)).trainW2V(train, validation)
  }

  def trainNaiveBoW() = {
    val (train, validation) = Fetcher.ordered()
    NaiveBayesTrainer(Config.tag.getOrElse(Config.prefsName), superSample = Config.superSample.getOrElse(false)).trainBoW(train, validation, termFrequencyThreshold = 100)
  }

}

sealed trait ModelTrainer {

  val count: String = if (Config.count == Int.MaxValue) "all" else Config.count.toString
  val prefix     : String
  val tag        : String
  val superSample: Boolean
  var feat: String = "UNINITIALIZED"

  lazy val name: String = s"${tag + "_"}${prefix}_$feat${if (superSample) "_super" else ""}${if (Config.count == Int.MaxValue) "_all" else "_" + Config.count}"

  def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int)

  def trainW2V(train: RDD[Article], validation: RDD[Article])

  def processTraining(train: RDD[Article], superSample: Boolean): (String => RDD[Article]) = {
    label => if (superSample) Cacher.supersampledBalanced(label, train) else train
  }
}

sealed case class FeedforwardTrainer(tag: String,
                                     learningRate: Seq[Double],
                                     minibatchSize: Seq[Int] = Seq(Config.miniBatchSize.getOrElse(1000)),
                                     superSample: Boolean = Config.superSample.getOrElse(false)
                                    ) extends ModelTrainer {

  override val prefix = "ffn"

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload()
    val tfidf = TFIDF(train, 0, name)
    NeuralTrainer.trainNetwork(
      CorpusDataset.genW2VDataset(validation, tfidf), label => CorpusDataset.genW2VDataset(processTraining(train, superSample)(label), tfidf), name, minibatchSize, learningRate,
      (neuralPrefs, iteratorPrefs) => binaryTrainer(FeedForward.create(neuralPrefs), neuralPrefs, iteratorPrefs)
    )
  }

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    feat = "bow"
    val tfidf = TFIDF(train, termFrequencyThreshold, name)
    val processedTraining: RDD[Article] = TFIDF.frequencyFilter(train, tfidf.phrases)
    val processedValidation: RDD[Article] = TFIDF.frequencyFilter(validation, tfidf.phrases)
    NeuralTrainer.trainNetwork(
      CorpusDataset.genBoWDataset(processedValidation, tfidf), label => CorpusDataset.genBoWDataset(processTraining(processedTraining, superSample)(label), tfidf), name, minibatchSize, learningRate,
      (neuralPrefs, iteratorPrefs) => binaryTrainer(FeedForward.createBoW(neuralPrefs, tfidf.phrases.size), neuralPrefs, iteratorPrefs)
    )
  }

  private def binaryTrainer(net: MultiLayerNetwork, neuralPrefs: NeuralPrefs, tw: IteratorPrefs): NeuralResult = {
    val trainIter = new FeedForwardIterator(tw.training, IPTC.topCategories.indexOf(tw.label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(tw.validation, IPTC.topCategories.indexOf(tw.label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.trainLabel(name, tw.label, neuralPrefs, net, trainIter, testIter)
  }
}

sealed case class RecurrentTrainer(tag: String,
                                   learningRate: Seq[Double],
                                   minibatchSize: Seq[Int] = Seq(Config.miniBatchSize.getOrElse(1000)),
                                   superSample: Boolean = Config.superSample.getOrElse(false),
                                   hiddenNodes: Int = Config.hidden1.getOrElse(10)
                                  ) extends ModelTrainer {

  override val prefix = "rnn"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = throw new IllegalStateException("RNN does not support BoW")

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload()
    val tfidf = TFIDF(train, 0, name)
    NeuralTrainer.trainNetwork(
      CorpusDataset.genW2VMatrix(validation, tfidf), label => CorpusDataset.genW2VMatrix(processTraining(train, superSample)(label), tfidf),
      name, minibatchSize, learningRate, binaryRNNTrainer
    )
  }

  // Binary trainers
  private def binaryRNNTrainer(neuralPrefs: NeuralPrefs, tw: IteratorPrefs): NeuralResult = {
    val realPrefs: NeuralPrefs = neuralPrefs.copy(hiddenNodes = hiddenNodes)
    val net = RNN.createBinary(realPrefs)
    val trainIter = new RNNIterator(tw.training, tw.label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(tw.validation, tw.label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.trainLabel(name, tw.label, realPrefs, net, trainIter, testIter)
  }
}

sealed case class NaiveBayesTrainer(tag: String, superSample: Boolean = false) extends ModelTrainer {

  override val prefix = "nb"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    feat = "bow"
    val tfidf = TFIDF(train, termFrequencyThreshold, name)
    trainNaiveBayes(TFIDF.frequencyFilter(train, tfidf.phrases), TFIDF.frequencyFilter(validation, tfidf.phrases), tfidf, superSample)
  }

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload()
    val tfidf = TFIDF(train, 0, name)
    trainNaiveBayes(train, validation, tfidf, superSample)
  }

  private def trainNaiveBayes(train: RDD[Article], validation: RDD[Article], tfidf: TFIDF, superSample: Boolean) = {
    val models: Map[String, NaiveBayesModel] = Config.cats.map(c => (c, MlLibUtils.multiLabelClassification(c, processTraining(train, superSample)(c), validation, tfidf))).toMap
    val predicted = MlLibUtils.testMLlibModels(validation, models, tfidf)
    val resultsFile: String = s"train/$name.txt"
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs(iteration = 0)), resultsFile, resultsFile)
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"$name/${name}_${IPTC.trim(c)}.bin") }
  }
}
