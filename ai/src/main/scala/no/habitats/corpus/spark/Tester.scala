package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.{NeuralModel, NeuralModelLoader, NeuralPredictor}
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.dl4j.NeuralEvaluation
import no.habitats.corpus.dl4j.networks.{FeedForwardIterator, RNNIterator}
import no.habitats.corpus.mllib.{MlLibUtils, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.DataSetIterator

import scala.util.Try

object Tester {

  def tester(name: String) = name match {
    case _ if name.contains("nb") => NaiveBayesTester(name)
    case _ if name.contains("rnn") => RecurrentTester(name)
    case _ if name.contains("ffn") => FeedforwardTester(name)
    case _ => throw new IllegalStateException(s"Illegal model: $name")
  }

  def testModels() = {
    Log.v("Testing models")
    val test = Fetcher.annotatedTestOrdered.map(_.toMinimal)
    tester("all-ffn-bow").test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-ffn-w2v").test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    tester("all-rnn-w2v").test(test, predict = true, shouldLogResults = Config.logResults.getOrElse(false))
    //    tester("all-nb-bow").test(test)
    tester("all-nb-w2v").test(test)
  }

  def verifyAll(): Boolean = new File(Config.modelPath).listFiles().map(_.getName).map(tester).forall(_.verify)

  def testSub() = {
    Log.v("Testing sub models")
    val test = Fetcher.subTestOrdered.map(_.toMinimal)
    val includeExampleBased = true
    tester("sub-rnn-w2v").test(test, includeExampleBased)
    tester("sub-ffn-w2v").test(test, includeExampleBased)
    tester("sub-ffn-bow").test(test, includeExampleBased)
    tester("sub-nb-bow").test(test, includeExampleBased)
    tester("sub-nb-w2v").test(test, includeExampleBased)
  }

  def testEmbeddedVsBoW() = {
    Log.v("Testing embedded vs. BoW")
    val test = Fetcher.annotatedTestOrdered.map(_.toMinimal)

    val predict = true
    tester("all-ffn-bow").test(test, predict)
    tester("all-nb-bow").test(test, predict)
    tester("all-ffn-w2v").test(test, predict)
    tester("all-nb-w2v").test(test, predict)
  }

  def testFFNBow() = {
    Log.v("Testing embedded vs. BoW")
    val test = Fetcher.annotatedTestOrdered

    tester("ffn-bow-all").test(test)
    tester("ffn-w2v-all").test(test)
  }

  def testTypesInclusion() = {
    Log.v("Testing type inclusion")
    val rddOrdered = Fetcher.subTestOrdered.map(_.toMinimal)

    Log.v("Annotations with types ...")
    tester("types-ffn-w2v").test(rddOrdered)
    tester("ffn-w2v").test(rddOrdered)

    //    Log.r("Normal annotations ...")
    //    FeedforwardTester("ffn-w2v-ordered").test(testOrdered)
  }

  def testShuffledVsOrdered() = {
    Log.v("Testing shuffled vs. ordered")

    // Shuffled
    Log.v("Shuffled ...")
    val rddShuffled = Fetcher.annotatedTestShuffled.map(_.toMinimal)
    FeedforwardTester("ffn-w2v-shuffled").test(rddShuffled)

    //    // Ordered
    //    Log.r("Ordered ...")
    //    val rddOrdered = Fetcher.annotatedTestOrdered
    //    val testOrdered = rddOrdered.collect()
    //    FeedforwardTester("ffn-w2v-ordered").test(testOrdered)
  }

  def testLengths() = {
    Log.v("Testing Lengths")
    testBuckets("length", tester("ffn-w2v-ordered"), _.wc)
    testBuckets("length", tester("nb-bow"), _.id.toInt)
    testBuckets("length", tester("nb-w2v"), _.id.toInt)
  }

  def testTimeDecay() = {
    Log.v("Testing Time Decay")
    //    testBuckets("time", FeedforwardTester("ffn-w2v-time-all"), _.id.toInt)
    testBuckets("time", tester("ffn-bow-time-all"), _.id.toInt)
    //    testBuckets("time", NaiveBayesTester("nb-bow"), _.id.toInt)
    //    testBuckets("time", NaiveBayesTester("nb-w2v"), _.id.toInt)
  }

  def testConfidence() = {
    Log.v("Testing Confidence Levels")
    for (confidence <- Seq(25, 50, 75, 100)) {
//      FeedforwardTester(s"confidence-${confidence}_ffn_bow_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"), predict = true)
      FeedforwardTester(s"confidence-${confidence}_ffn_w2v_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"), predict = true)
      NaiveBayesTester(s"confidence-${confidence}_nb_w2v_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"))
      NaiveBayesTester(s"confidence-${confidence}_nb_bow_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"))
    }
  }

  /** Test model on every test set matching name */
  def testBuckets(name: String, tester: Testable, criterion: Article => Double) = {
    val rdds: Array[(Int, RDD[Article])] = new File(Config.dataPath + s"nyt/$name/").listFiles
      .map(_.getName).filter(_.contains(s"test"))
      .map(n => (n.split("_").filter(s => Try(s.toInt).isSuccess).head.toInt, n))
      .map { case (index, n) => (index, Fetcher.fetch(s"nyt/$name/$n")) }
      .sortBy(_._1)
    rdds.foreach { case (index, n) => {
      Log.toFile(s"${name} group: $index -  min: ${Try(n.map(criterion).min).getOrElse("N/A")} - max: ${Try(n.map(criterion).max).getOrElse("N/A")}", s"test/$name.txt")
      tester.test(n, iteration = index)
    }
    }
  }
}

sealed trait Testable {

  import scala.collection.JavaConverters._

  val name: String

  def iter(test: CorpusDataset, label: String): DataSetIterator = ???

  def models(modelName: String): Map[String, NeuralModel] = ???

  def verify: Boolean = Try(models(name)).isSuccess

  def test(test: RDD[Article], predict: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    test.persist()
    Log.v(s"Testing ${test.count} articles ...")
    val predictedArticles: Option[RDD[Article]] = if (predict) Some(predictAll(test)) else None

    val resultFile = s"test/$name.txt"
    val labelEvals: Seq[NeuralEvaluation] = labelBased(test, iteration)
    NeuralEvaluation.logLabelStats(labelEvals, resultFile)
    NeuralEvaluation.log(labelEvals, resultFile, IPTC.topCategories, iteration, predictedArticles)
    if (shouldLogResults) predictedArticles.foreach(logResults)
    test.unpersist()
    System.gc()
  }

  def logResults(articles: RDD[Article]) = {
    articles.sortBy(_.id).foreach(a => {
      a.pred.foreach(p => {
        val folder: String = Config.dataPath + s"predictions/$name/" + (if (a.iptc.contains(p)) "tp" else "fp")
        Log.saveToFile(a.toStringFull, p, folder)
      })
    })
  }

  def dataset(test: RDD[Article]): CorpusDataset = {
    val tfidf = TFIDF.deserialize(name)
    if (name.contains("bow")) CorpusDataset.genBoWDataset(test, tfidf) else CorpusDataset.genW2VDataset(test, tfidf)
  }

  def predictAll(test: RDD[Article]): RDD[Article] = {
    val modelType = if (name.toLowerCase.contains("bow")) Some(TFIDF.deserialize(name)) else None
    NeuralPredictor.predict(test, models(name), modelType)
  }

  def labelBased(test: RDD[Article], iteration: Int): Seq[NeuralEvaluation] = {
    if (iteration == 0) Log.toFile(s"Testing $name ...", s"test/$name.txt")
    val testDataset: CorpusDataset = dataset(test)
    models(name).toSeq.sortBy(_._1).zipWithIndex.map { case (models, i) => {
      val test = iter(testDataset, models._1).asScala.toTraversable
      val eval = NeuralEvaluation(test, models._2.network, i, models._1)
      eval
    }
    }
  }
}

case class FeedforwardTester(name: String) extends Testable {
  lazy val ffa: Map[String, NeuralModel] = NeuralModelLoader.models(name)

  override def iter(test: CorpusDataset, label: String): DataSetIterator = new FeedForwardIterator(test.asInstanceOf[CorpusVectors], IPTC.topCategories.indexOf(label), 500)
  override def models(modelName: String): Map[String, NeuralModel] = ffa
}

case class RecurrentTester(name: String) extends Testable {
  lazy val rnn: Map[String, NeuralModel] = NeuralModelLoader.models(name)

  override def iter(test: CorpusDataset, label: String): DataSetIterator = new RNNIterator(test.asInstanceOf[CorpusMatrix], label, 50)
  override def models(modelName: String): Map[String, NeuralModel] = rnn
}

case class NaiveBayesTester(name: String) extends Testable {
  lazy val nb: Map[String, NaiveBayesModel] = IPTC.topCategories.map(c => (c, MLlibModelLoader.load(name, IPTC.trim(c)))).toMap

  override def verify: Boolean = Try(nb).isSuccess

  override def test(articles: RDD[Article], includeExampleBased: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    Log.toFile(s"Testing Naive Bayes [$name] ...", s"test/$name.txt")
    val predicted = MlLibUtils.testMLlibModels(articles, nb, if (name.contains("bow")) Some(TFIDF.deserialize(name)) else None)

    Log.v("--- Predictions complete! ")
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs()), s"test/$name.txt")
    if (shouldLogResults) logResults(predicted)
    System.gc()
  }
}
