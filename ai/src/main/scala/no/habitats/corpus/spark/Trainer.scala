package no.habitats.corpus.spark

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, _}
import no.habitats.corpus.dl4j.NeuralTrainer.NeuralResult
import no.habitats.corpus.dl4j.networks.{FeedForward, FeedForwardIterator, RNN, RNNIterator}
import no.habitats.corpus.dl4j.{NeuralEvaluation, NeuralPrefs, NeuralTrainer}
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
    NaiveBayesTrainer(tag = Some("bow-sub")).trainBoW(train, validation, termFrequencyThreshold = 5)
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
      val tag: Some[String] = Some(s"confidence-$confidence")
      Log.v(s"Training with confidence ${confidence} ...")
      FeedforwardTrainer(tag = tag).trainW2V(train = train(confidence), validation = validation(confidence))
      FeedforwardTrainer(tag = tag).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
      //      NaiveBayesTrainer(tag = tag).trainW2V(train = train(confidence), validation = validation(confidence))
      //      NaiveBayesTrainer(tag = tag).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
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
  var feat: String = "UNINITIALIZED"

  lazy val name: String = {
    val name = s"${tag.map(_ + "_").getOrElse("")}${prefix}_$feat${if (superSample) "_super" else ""}${if (Config.count == Int.MaxValue) "_all" else "_" + Config.count}"
    Config.resultsFileName = s"train/$name.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    name
  }

  def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int = 100)

  def trainW2V(train: RDD[Article], validation: RDD[Article])

  def processTraining(train: RDD[Article], superSample: Boolean): (String => RDD[Article]) = {
    label => if (superSample) Cacher.supersampledBalanced(label, train) else train
  }
}

sealed trait NeuralTrainer {

  case class IteratorPrefs(label: String, training: CorpusDataset, validation: CorpusDataset)

  def trainNetwork(validation: CorpusDataset, training: (String) => CorpusDataset, name: String, minibatchSize: Int, learningRate: Seq[Double], tp: (NeuralPrefs, IteratorPrefs) => NeuralResult) = {
    if (Config.parallelism > 1) parallel(validation, training(""), name, minibatchSize, learningRate, tp, Config.parallelism)
    else sequential(validation, training, name, minibatchSize, learningRate, tp)
  }

  def sequential(validation: CorpusDataset, training: (String) => CorpusDataset, name: String, minibatchSize: Int, learningRate: Seq[Double], trainer: (NeuralPrefs, IteratorPrefs) => NeuralResult) = {
    for {lr <- learningRate} {
      val prefs = NeuralPrefs(learningRate = lr, epochs = 1, minibatchSize = minibatchSize)
      val allRes: Seq[Seq[NeuralEvaluation]] = Config.cats.map(c => {
        val trainingPrefs: IteratorPrefs = IteratorPrefs(c, training(c), validation)
        val res: NeuralResult = trainer(prefs, trainingPrefs)
        NeuralModelLoader.save(res.net, c, Config.count, name)
        res.evaluations
      })
      printResults(allRes, name)
    }
  }

  def parallel(validation: CorpusDataset, train: CorpusDataset, name: String, minibatchSize: Int, learningRate: Seq[Double], trainer: (NeuralPrefs, IteratorPrefs) => NeuralResult, parallelism: Int) = {
    // Force pre-generation of document vectors before entering Spark to avoid passing W2V references between executors
    Log.v("Broadcasting dataset ...")
    val sparkTrain = sc.broadcast(train)
    val sparkValidation = sc.broadcast(train)
    Log.v("Starting distributed training ...")
    val eval = for {lr <- learningRate} yield {
      val allRes: Seq[Seq[NeuralEvaluation]] = sc.parallelize(Config.cats, numSlices = Math.min(Config.parallelism, Config.cats.size)).map(c => {
        val prefs: NeuralPrefs = NeuralPrefs(learningRate = lr, epochs = 1, minibatchSize = minibatchSize)
        val trainingPrefs: IteratorPrefs = IteratorPrefs(c, sparkTrain.value, sparkValidation.value)
        (c, trainer(prefs, trainingPrefs))
      }).map { case (c, res) => NeuralModelLoader.save(res.net, c, Config.count, name); res.evaluations }.collect().toSeq
      printResults(allRes, name)
    }
    sparkTrain.destroy()
    sparkValidation.destroy()
    eval
  }

  def printResults(allRes: Seq[Seq[NeuralEvaluation]], name: String) = {
    val iterations = allRes.head.size
    Log.v("Accumulating results ...")
    Config.resultsFileName = s"valid/$name.txt"
    Config.resultsCatsFileName = Config.resultsFileName
    Log.result("")
    Log.result(name)
    for (i <- 0 until iterations) {
      val labelEvals: Seq[NeuralEvaluation] = allRes.map(_(i)).sortBy(_.label)
      Log.result("")
      Log.result(s"Category stats epoch $i ...\n")
      labelEvals.sortBy(_.label).zipWithIndex.foreach{case (e, i) => e.log("valid", name, header = i == 0)}
      Log.result("")
      NeuralEvaluation.log(labelEvals, Config.cats, i)
    }
  }
}

sealed case class FeedforwardTrainer(
                                      learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)),
                                      minibatchSize: Int = Config.miniBatchSize.getOrElse(1000),
                                      superSample: Boolean = Config.superSample.getOrElse(false),
                                      tag: Option[String] = None
                                    ) extends ModelTrainer with NeuralTrainer {

  override val prefix = "ffn"

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    trainNetwork(
      CorpusDataset.genW2VDataset(validation), label => CorpusDataset.genW2VDataset(processTraining(train, superSample)(label)), name, minibatchSize, learningRate,
      (neuralPrefs, iteratorPrefs) => binaryTrainer(FeedForward.create(neuralPrefs), neuralPrefs, iteratorPrefs)
    )
  }

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int = 100) = {
    feat = "bow"
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    val processedValidation: RDD[Article] = TFIDF.frequencyFilter(validation, tfidf.phrases)
    val processedTraining: RDD[Article] = TFIDF.frequencyFilter(train, tfidf.phrases)
    trainNetwork(
      CorpusDataset.genBoWDataset(processedValidation, tfidf), label => CorpusDataset.genBoWDataset(processTraining(processedTraining, superSample)(label), tfidf), name, minibatchSize, learningRate,
      (neuralPrefs, iteratorPrefs) => binaryTrainer(FeedForward.createBoW(neuralPrefs, tfidf.phrases.size), neuralPrefs, iteratorPrefs)
    )
  }

  private def binaryTrainer(net: MultiLayerNetwork, neuralPrefs: NeuralPrefs, tw: IteratorPrefs): NeuralResult = {
    val trainIter = new FeedForwardIterator(tw.training, IPTC.topCategories.indexOf(tw.label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(tw.validation, IPTC.topCategories.indexOf(tw.label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(name, tw.label, neuralPrefs, net, trainIter, testIter)
  }
}

sealed case class RecurrentTrainer(
                                    learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)),
                                    minibatchSize: Int = Config.miniBatchSize.getOrElse(1000),
                                    superSample: Boolean = Config.superSample.getOrElse(false),
                                    hiddenNodes: Int = Config.hidden1.getOrElse(10),
                                    tag: Option[String] = None) extends ModelTrainer with NeuralTrainer {

  override val prefix = "rnn"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = throw new IllegalStateException("RNN does not support BoW")

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    trainNetwork(CorpusDataset.genW2VDataset(validation), label => CorpusDataset.genW2VDataset(processTraining(train, superSample)(label)), name, minibatchSize, learningRate, binaryRNNTrainer)
  }

  // Binary trainers
  private def binaryRNNTrainer(neuralPrefs: NeuralPrefs, tw: IteratorPrefs): NeuralResult = {
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    val net = RNN.createBinary(neuralPrefs.copy(hiddenNodes = hiddenNodes))
    val trainIter = new RNNIterator(tw.training.articles, Some(tw.label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(tw.validation.articles, Some(tw.label), batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(name, tw.label, neuralPrefs, net, trainIter, testIter)
  }
}

sealed case class NaiveBayesTrainer(superSample: Boolean = false, tag: Option[String] = None) extends ModelTrainer {

  override val prefix = "nb"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    feat = "bow"
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.toFile(TFIDF.serialize(tfidf), name + "-" + count + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    trainNaiveBayes(TFIDF.frequencyFilter(train, tfidf.phrases), TFIDF.frequencyFilter(validation, tfidf.phrases), Some(tfidf), superSample)
  }

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    trainNaiveBayes(train, validation, None, superSample)
  }

  private def trainNaiveBayes(train: RDD[Article], validation: RDD[Article], tfidf: Option[TFIDF] = None, superSample: Boolean) = {
    val models: Map[String, NaiveBayesModel] = Config.cats.map(c => (c, MlLibUtils.multiLabelClassification(c, processTraining(train, superSample)(c), validation, tfidf))).toMap
    val predicted = MlLibUtils.testMLlibModels(validation, models, tfidf)
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs()))
    val fullName = name + (if (Config.count != Int.MaxValue) s"_$count" else "")
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"$fullName/${name}_${IPTC.trim(c)}.bin") }
  }
}
