package no.habitats.corpus.spark

import no.habitats.corpus.{ArticleUtils, Corpus}
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.{Annotation, Article, CorpusDataset}
import no.habitats.corpus.common.{Config, _}
import no.habitats.corpus.dl4j.NeuralTrainer.{IteratorPrefs, NeuralResult}
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralPrefs, NeuralTrainer}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import no.habitats.corpus.nlp.extractors.Simple
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.language.implicitConversions

object Trainer extends Serializable {

  implicit def seqthisDouble(a: Double): Seq[Double] = Seq(a)
  implicit def seqthisInt(a: Int): Seq[Int] = Seq(a)

  // Ex1/2 - Baseline
  def baseline() = {
    val (train, validation) = Fetcher.ordered()
    val tag = Tester.baseline
    val learningRate = 0.05
    FeedforwardTrainer(tag, learningRate).trainW2V(train, validation)
    FeedforwardTrainer(tag, learningRate).trainBoW(train, validation, termFrequencyThreshold = 100)
    NaiveBayesTrainer(tag).trainW2V(train, validation)
    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 100)
    RecurrentTrainer(tag, learningRate).trainW2V(train, validation)
  }

  def supersampled() = {
    val (train, validation) = Fetcher.ordered()
    val tag = Tester.superSample
    val learningRate = 0.05
    FeedforwardTrainer(tag, learningRate, superSample = true).trainW2V(train, validation)
    FeedforwardTrainer(tag, learningRate, superSample = true).trainBoW(train, validation, termFrequencyThreshold = 100)
    NaiveBayesTrainer(tag, superSample = true).trainW2V(train, validation)
    NaiveBayesTrainer(tag, superSample = true).trainBoW(train, validation, termFrequencyThreshold = 100)
    RecurrentTrainer(tag, learningRate, superSample = true).trainW2V(train, validation)
  }

  // Ex3 - Types
  def types() = {
    val tag = Tester.types
    val (train, validation) = Fetcher.ordered(types = true)
    val learningRate = 0.05
    val termFrequencyThreshold = 100
    //    FeedforwardTrainer(tag, learningRate, 250).trainW2V(train, validation)
    NaiveBayesTrainer(tag).trainW2V(train, validation)
    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold)
    //    FeedforwardTrainer(tag, learningRate, 250).trainBoW(train, validation, termFrequencyThreshold)
    //    RecurrentTrainer(tag, learningRate, 50).trainW2V(train, validation)
  }

  // Ex5 - Time
  def time() = {
    val train = Fetcher.by("time/nyt_time_10_train.txt")
    val validation = Fetcher.by("time/nyt_time_10-0_validation.txt")
    val tag = Tester.time
    val learningRate = 0.5
    FeedforwardTrainer(tag, learningRate).trainW2V(train, validation)
    FeedforwardTrainer(tag, learningRate).trainBoW(train, validation, termFrequencyThreshold = 20)
    NaiveBayesTrainer(tag).trainW2V(train, validation)
    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 20)
    RecurrentTrainer(tag, learningRate).trainW2V(train, validation)
  }

  // Ex2 - Confidence
  def confidence() = {
    def train(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_train_ordered_${confidence}.txt", 0.6).map(_.toMinimal)
    def validation(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_validation_ordered_${confidence}.txt", 0.2).map(_.toMinimal)
    def tag(confidence: Int): String = s"confidence-$confidence"

    val learningRates = Seq(0.5)
    var confidence = 25
    val termFrequencyThreshold: Int = 10
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    FeedforwardTrainer(tag(confidence), learningRates, 250).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence), learningRates, 250).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    RecurrentTrainer(tag(confidence), learningRates, 50).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 50
    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    FeedforwardTrainer(tag(confidence), learningRates, 250).trainW2V(train = train(confidence), validation = validation(confidence))
    FeedforwardTrainer(tag(confidence), learningRates, 250).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    RecurrentTrainer(tag(confidence), learningRates, 50).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 75
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    FeedforwardTrainer(tag(confidence), learningRates, 250).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence), learningRates, 250).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    RecurrentTrainer(tag(confidence), learningRates, 50).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 100
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    FeedforwardTrainer(tag(confidence), learningRates, 250).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence), learningRates, 250).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = termFrequencyThreshold)
    //    RecurrentTrainer(tag(confidence), learningRates, 50).trainW2V(train = train(confidence), validation = validation(confidence))
  }

  def trainNaiveTraditional() = {
    val train = Fetcher.corpusTrainOrdered.map(Corpus.toTraditional)
    val test = Fetcher.corpusValidationOrdered.map(Corpus.toTraditional)
    NaiveBayesTrainer("traditional").trainBoW(train, test, 200)
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

  def trainIncrementalFFN() = {
    for(i <- 5000 until 200000 by 5000) {
      val train = sc.parallelize(Fetcher.annotatedTrainOrdered.take(i))
      val validation = sc.parallelize(Fetcher.annotatedValidationOrdered.take(50000))
      FeedforwardTrainer("incremental", Seq(Config.learningRate.getOrElse(0.05)), superSample = Config.superSample.getOrElse(false)).trainW2V(train, validation)
    }
  }

  def trainIncrementalFFN2() = {
    for(i <- 10000 until 200000 by 10000) {
      val train = sc.parallelize(Fetcher.annotatedTrainOrdered.take(i))
      val validation = sc.parallelize(Fetcher.annotatedValidationOrdered.take(50000))
      FeedforwardTrainer("incremental", Seq(Config.learningRate.getOrElse(0.05)), superSample = Config.superSample.getOrElse(false)).trainW2V(train, validation)
    }
  }

  def trainIncrementalNB() = {
    for(i <- 5000 until 200000 by 5000) {
      val train = sc.parallelize(Fetcher.annotatedTrainOrdered.take(i))
      val validation = Fetcher.annotatedValidationOrdered
      NaiveBayesTrainer("incremental", superSample = Config.superSample.getOrElse(false)).trainW2V(train, validation)
    }
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

  lazy val name    : String = s"${prefix}_$feat${if (superSample) "_super" else ""}${if (Config.count == Int.MaxValue || Config.pretrained) "_all" else "_" + Config.count}"
  lazy val modelDir: String = Config.modelDir(name, tag)
  lazy val trainDir: String = Config.trainDir(name, tag)

  def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int)

  def trainW2V(train: RDD[Article], validation: RDD[Article])

  def processTraining(train: RDD[Article], superSample: Boolean): (String => RDD[Article]) = {
    label => if (superSample) Cacher.supersampledBalanced(label, train) else train
  }
}

sealed case class FeedforwardTrainer(tag: String,
                                     learningRate: Seq[Double],
                                     minibatchSize: Seq[Int] = Seq(Config.miniBatchSize.getOrElse(250)),
                                     superSample: Boolean = Config.superSample.getOrElse(false)
                                    ) extends ModelTrainer {

  override val prefix = "ffn"

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload()
    val tfidf = TFIDF(train, 0, modelDir)
    NeuralTrainer.trainNetwork(
      CorpusDataset.genW2VDataset(validation, tfidf), label => CorpusDataset.genW2VDataset(processTraining(train, superSample)(label), tfidf),
      name, tag, minibatchSize, learningRate,
      (neuralPrefs, iteratorPrefs) => binaryTrainer(FeedForward.create(neuralPrefs), neuralPrefs, iteratorPrefs)
    )
  }

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    feat = "bow"
    val tfidf = TFIDF(train, termFrequencyThreshold, modelDir)
    val processedTraining: RDD[Article] = TFIDF.frequencyFilter(train, tfidf.phrases)
    val processedValidation: RDD[Article] = TFIDF.frequencyFilter(validation, tfidf.phrases)
    NeuralTrainer.trainNetwork(
      CorpusDataset.genBoWDataset(processedValidation, tfidf), label => CorpusDataset.genBoWDataset(processTraining(processedTraining, superSample)(label), tfidf),
      name, tag, minibatchSize, learningRate,
      (neuralPrefs, iteratorPrefs) => binaryTrainer(FeedForward.createBoW(neuralPrefs, tfidf.phrases.size), neuralPrefs, iteratorPrefs)
    )
  }

  private def binaryTrainer(net: MultiLayerNetwork, neuralPrefs: NeuralPrefs, tw: IteratorPrefs): NeuralResult = {
    val trainIter = new FeedForwardIterator(tw.training, IPTC.topCategories.indexOf(tw.label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(tw.validation, IPTC.topCategories.indexOf(tw.label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.trainLabel(name, tag, tw.label, neuralPrefs, net, trainIter, testIter)
  }
}

sealed case class RecurrentTrainer(tag: String,
                                   learningRate: Seq[Double],
                                   minibatchSize: Seq[Int] = Seq(Config.miniBatchSize.getOrElse(50)),
                                   superSample: Boolean = Config.superSample.getOrElse(false),
                                   hiddenNodes: Int = Config.hidden1.getOrElse(10)
                                  ) extends ModelTrainer {

  override val prefix = "rnn"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = throw new IllegalStateException("RNN does not support BoW")

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload()
    val tfidf = TFIDF(train, 0, modelDir)
    val validationMatrix: CorpusDataset = CorpusDataset.genW2VMatrix(validation, tfidf)
    val trainingMatrix: CorpusDataset = CorpusDataset.genW2VMatrix(train, tfidf)
    NeuralTrainer.trainNetwork(validationMatrix, label => trainingMatrix, name, tag, minibatchSize, learningRate, binaryRNNTrainer)
  }

  // Binary trainers
  private def binaryRNNTrainer(neuralPrefs: NeuralPrefs, tw: IteratorPrefs): NeuralResult = {
    val realPrefs: NeuralPrefs = neuralPrefs.copy(hiddenNodes = hiddenNodes)
    val net = if (Config.pretrained) RNN.create(realPrefs, NeuralModelLoader.model(name, tag, tw.label)) else RNN.create(realPrefs)
    val trainIter = new RNNIterator(tw.training, tw.label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(tw.validation, tw.label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.trainLabel(name, tag, tw.label, realPrefs, net, trainIter, testIter)
  }
}

sealed case class NaiveBayesTrainer(tag: String, superSample: Boolean = false) extends ModelTrainer {

  override val prefix = "nb"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    feat = "bow"
    val tfidf = TFIDF(train, termFrequencyThreshold, modelDir)
    trainNaiveBayes(TFIDF.frequencyFilter(train, tfidf.phrases), TFIDF.frequencyFilter(validation, tfidf.phrases), tfidf, superSample)
  }

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload()
    val tfidf = TFIDF(train, 0, modelDir)
    trainNaiveBayes(train, validation, tfidf, superSample)
  }

  private def trainNaiveBayes(train: RDD[Article], validation: RDD[Article], tfidf: TFIDF, superSample: Boolean) = {
    val start = System.currentTimeMillis()
    val resultsFile: String = trainDir + s"$name.txt"
    val decay = Config.decay
    val v = (if (decay) sc.parallelize(validation.take(2000)) else validation).map(_.filterAnnotation(an => tfidf.contains(an.id))).filter(_.ann.nonEmpty)
    for {
      i <- if (decay) 1 until 100 else Seq(100)
    } yield {
      val t = train.map(_.filterAnnotation(an => tfidf.contains(an.id))).filter(_.ann.nonEmpty).sample(withReplacement = false, i / 100.toDouble, Config.seed)
      val models: Map[String, NaiveBayesModel] = Config.cats.map(c => (c, MlLibUtils.multiLabelClassification(c, processTraining(t, superSample)(c), v, tfidf))).toMap
      val predicted = MlLibUtils.testMLlibModels(v, models, tfidf)
      MlLibUtils.evaluate(predicted, sc.broadcast(Prefs(iteration = 0)), resultsFile, resultsFile)
      models.foreach { case (c, model) => MLlibModelLoader.save(model, modelDir + s"${name}_${IPTC.trim(c)}.bin") }
    }
    Log.toFile(s"Training finished in ${SparkUtil.prettyTime(System.currentTimeMillis() - start)}", resultsFile)
  }
}
