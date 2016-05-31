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

import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}
import scala.language.implicitConversions

object Trainer extends Serializable {

  implicit def seqthis(a: Double): Seq[Double] = Seq(a)

  // Ex1/2 - Baseline
  def baseline() = {
    val (train, validation) = Fetcher.ordered()
    val tag = Some("baseline")
    //    FeedforwardTrainer(tag).trainW2V(train, validation)
    FeedforwardTrainer(tag).trainBoW(train, validation)
    //    NaiveBayesTrainer(tag).trainW2V(train, validation)
    //    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 100)
    RecurrentTrainer(tag).trainW2V(train, validation)
  }

  def supersampled() = {
    val (train, validation) = Fetcher.ordered()
    val tag = Some("super")
    FeedforwardTrainer(tag, superSample = true).trainW2V(train, validation)
    FeedforwardTrainer(tag, superSample = true).trainBoW(train, validation, termFrequencyThreshold = 100)
    //    NaiveBayesTrainer(tag, superSample = true).trainW2V(train, validation)
    //    NaiveBayesTrainer(tag, superSample = true).trainBoW(train, validation, termFrequencyThreshold = 100)
    RecurrentTrainer(tag, superSample = true).trainW2V(train, validation)
  }

  // Ex3 - Types
  def types() = {
    val (train, validation) = Fetcher.ordered(types = true)
    val tag = Some("types")
    FeedforwardTrainer(tag).trainW2V(train, validation)
    FeedforwardTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 100)
    //    NaiveBayesTrainer(tag).trainW2V(train, validation)
    //    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 100)
    RecurrentTrainer(tag).trainW2V(train, validation)
  }

  // Ex5 - Time
  def time() = {
    val train = Fetcher.by("time/nyt_time_10_train.txt")
    val validation = Fetcher.by("time/nyt_time_10-0_validation.txt")
    val tag = Some("time")
    FeedforwardTrainer(tag).trainW2V(train, validation)
    FeedforwardTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 5)
    NaiveBayesTrainer(tag).trainW2V(train, validation)
    NaiveBayesTrainer(tag).trainBoW(train, validation, termFrequencyThreshold = 5)
    RecurrentTrainer(tag).trainW2V(train, validation)
  }

  // Ex2 - Confidence
  def confidence() = {
    def train(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_train_ordered_${confidence}.txt")
    def validation(confidence: Int): RDD[Article] = Fetcher.by(s"confidence/nyt_mini_validation_ordered_${confidence}.txt")
    def tag(confidence: Int): Some[String] = Some(s"confidence-$confidence")

    val learningRates = Seq(0.5)
    var confidence = 25
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    FeedforwardTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    FeedforwardTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    RecurrentTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 50
    //    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    //    FeedforwardTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    FeedforwardTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    RecurrentTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 75
//    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    //    FeedforwardTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    FeedforwardTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    RecurrentTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))

    confidence = 100
//    NaiveBayesTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    //    NaiveBayesTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    FeedforwardTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
    FeedforwardTrainer(tag(confidence)).trainBoW(train = train(confidence), validation = validation(confidence), termFrequencyThreshold = 10)
    RecurrentTrainer(tag(confidence)).trainW2V(train = train(confidence), validation = validation(confidence))
  }

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

}

sealed trait ModelTrainer {

  val count: String = if (Config.count == Int.MaxValue) "all" else Config.count.toString
  val prefix     : String
  val tag        : Option[String]
  val superSample: Boolean
  var feat: String = "UNINITIALIZED"

  lazy val name: String = s"${tag.map(_ + "_").getOrElse("")}${prefix}_$feat${if (superSample) "_super" else ""}${if (Config.count == Int.MaxValue) "_all" else "_" + Config.count}"

  def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int = 100)

  def trainW2V(train: RDD[Article], validation: RDD[Article])

  def processTraining(train: RDD[Article], superSample: Boolean): (String => RDD[Article]) = {
    label => if (superSample) Cacher.supersampledBalanced(label, train) else train
  }
}

sealed trait NeuralTrainer {

  case class IteratorPrefs(label: String, training: CorpusDataset, validation: CorpusDataset)

  def trainNetwork(validation: CorpusDataset, training: (String) => CorpusDataset, name: String, minibatchSize: Seq[Int], learningRate: Seq[Double], tp: (NeuralPrefs, IteratorPrefs) => NeuralResult) = {
    val resultFile = s"train/$name.txt"
    Log.toFile("", resultFile)
    Log.toFile("", resultFile)
    Log.toFile("", resultFile)
    Log.toFile("Model: " + name + " - Args: " + Config.getArgs, resultFile)
    Log.toFile("", resultFile)
    if (Config.parallelism > 1) parallel(validation, training(""), name, minibatchSize, learningRate, tp, Config.parallelism)
    else sequential(validation, training, name, minibatchSize, learningRate, tp)
  }

  def sequential(validation: CorpusDataset, training: (String) => CorpusDataset, name: String, minibatchSize: Seq[Int], learningRate: Seq[Double], trainer: (NeuralPrefs, IteratorPrefs) => NeuralResult) = {
    for {lr <- learningRate; mbs <- minibatchSize} {
      val prefs = NeuralPrefs(learningRate = lr, epochs = 1, minibatchSize = mbs)
      val allRes: Seq[Seq[NeuralEvaluation]] = Config.cats.map(c => {
        val trainingPrefs: IteratorPrefs = IteratorPrefs(c, training(c), validation)
        val res: NeuralResult = trainer(prefs, trainingPrefs)
        NeuralModelLoader.save(res.net, c, Config.count, name)
        res.evaluations
      })
      printResults(allRes, name)
    }
  }

  def parallel(validation: CorpusDataset, train: CorpusDataset, name: String, minibatchSize: Seq[Int], learningRate: Seq[Double], trainer: (NeuralPrefs, IteratorPrefs) => NeuralResult, parallelism: Int) = {
    // Force pre-generation of document vectors before entering Spark to avoid passing W2V references between executors
    Log.v("Broadcasting dataset ...")
    Log.v("Starting distributed training ...")
    val cats = Config.cats.par
    cats.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(Config.parallelism))
    // TODO: SPARK THIS UP, BUT DON'T FORGET THE W2V LOADER!
    for {lr <- learningRate; mbs <- minibatchSize} {
      val allRes: ParSeq[Seq[NeuralEvaluation]] = cats.map(c => {
        val prefs: NeuralPrefs = NeuralPrefs(learningRate = lr, epochs = 1, minibatchSize = mbs)
        val trainingPrefs: IteratorPrefs = IteratorPrefs(c, train, validation)
        (c, trainer(prefs, trainingPrefs))
      }).map { case (c, res) => NeuralModelLoader.save(res.net, c, Config.count, name); res.evaluations }
      printResults(allRes.seq, name)
    }
  }

  def printResults(allRes: Seq[Seq[NeuralEvaluation]], name: String) = {
    val epochs = allRes.head.size
    Log.v("Accumulating results ...")
    val resultFile = s"train/$name.txt"
    Log.toFile("", resultFile)
    Log.toFile("Model: " + name + " - Args: " + Config.getArgs, resultFile)
    for (i <- 0 until epochs) {
      val labelEvals: Seq[NeuralEvaluation] = allRes.map(_ (i)).sortBy(_.label)
      NeuralEvaluation.logLabelStats(labelEvals, resultFile)
      NeuralEvaluation.log(labelEvals, resultFile, Config.cats, i)
    }
  }
}

sealed case class FeedforwardTrainer(tag: Option[String] = None,
                                     learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)),
                                     minibatchSize: Seq[Int] = Seq(Config.miniBatchSize.getOrElse(1000)),
                                     superSample: Boolean = Config.superSample.getOrElse(false)
                                    ) extends ModelTrainer with NeuralTrainer {

  override val prefix = "ffn"

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload(wordVectors = true, documentVectors = true)
    val tfidf = TFIDF(train, 0)
    trainNetwork(
      CorpusDataset.genW2VDataset(validation, tfidf), label => CorpusDataset.genW2VDataset(processTraining(train, superSample)(label), tfidf), name, minibatchSize, learningRate,
      (neuralPrefs, iteratorPrefs) => binaryTrainer(FeedForward.create(neuralPrefs), neuralPrefs, iteratorPrefs)
    )
  }

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    feat = "bow"
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.saveToFile(TFIDF.serialize(tfidf), name + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
    val processedTraining: RDD[Article] = TFIDF.frequencyFilter(train, tfidf.phrases)
    val processedValidation: RDD[Article] = TFIDF.frequencyFilter(validation, tfidf.phrases)
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

sealed case class RecurrentTrainer(tag: Option[String] = None,
                                   learningRate: Seq[Double] = Seq(Config.learningRate.getOrElse(0.05)),
                                   minibatchSize: Seq[Int] = Seq(Config.miniBatchSize.getOrElse(1000)),
                                   superSample: Boolean = Config.superSample.getOrElse(false),
                                   hiddenNodes: Int = Config.hidden1.getOrElse(10)
                                  ) extends ModelTrainer with NeuralTrainer {

  override val prefix = "rnn"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = throw new IllegalStateException("RNN does not support BoW")

  override def trainW2V(train: RDD[Article], validation: RDD[Article]) = {
    feat = "w2v"
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    val tfidf = TFIDF(train, 100)
    trainNetwork(
      CorpusDataset.genW2VMatrix(validation, tfidf),
      label => CorpusDataset.genW2VMatrix(processTraining(train, superSample)(label), tfidf),
      name, minibatchSize, learningRate, binaryRNNTrainer
    )
  }

  // Binary trainers
  private def binaryRNNTrainer(neuralPrefs: NeuralPrefs, tw: IteratorPrefs): NeuralResult = {
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    val realPrefs: NeuralPrefs = neuralPrefs.copy(hiddenNodes = hiddenNodes)
    val net = RNN.createBinary(realPrefs)
    val trainIter = new RNNIterator(tw.training, tw.label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(tw.validation, tw.label, batchSize = neuralPrefs.minibatchSize)
    NeuralTrainer.train(name, tw.label, realPrefs, net, trainIter, testIter)
  }
}

sealed case class NaiveBayesTrainer(tag: Option[String] = None, superSample: Boolean = false) extends ModelTrainer {

  override val prefix = "nb"

  override def trainBoW(train: RDD[Article], validation: RDD[Article], termFrequencyThreshold: Int) = {
    feat = "bow"
    W2VLoader.preload(wordVectors = true, documentVectors = false)
    val tfidf = TFIDF(train, termFrequencyThreshold)
    Log.saveToFile(TFIDF.serialize(tfidf), name + "/" + name + "-tfidf.txt", Config.cachePath, overwrite = true)
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
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs()), s"train/$name.txt")
    models.foreach { case (c, model) => MLlibModelLoader.save(model, s"$name/${name}_${IPTC.trim(c)}.bin") }
  }
}
