package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.dl4j.{NeuralModel, NeuralModelLoader, NeuralPredictor}
import no.habitats.corpus.common.mllib.MLlibModelLoader
import no.habitats.corpus.common.models.{Article, CorpusDataset}
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
//    testBuckets("_length", tester("_baseline/nb_bow_all"), _.wc)
    testBuckets("_length", tester("_baseline/nb_w2v_all"), _.wc)
    testBuckets("_length", tester("_baseline/ffn_w2v_all"), _.wc)
//    testBuckets("_length", tester("_baseline/ffn_bow_all"), _.wc)
  }

  def testTimeDecay() = {
    Log.v("Testing Time Decay")
    val path: String = "_time"
    testBuckets(path, tester(path + "/time_ffn_w2v_all"), _.id.toInt)
    testBuckets(path, tester(path + "/time_ffn_bow_all"), _.id.toInt)
    testBuckets(path, tester(path + "/time_nb_bow_all"), _.id.toInt)
    testBuckets(path, tester(path + "/time_nb_w2v_all"), _.id.toInt)
  }

  def testConfidence() = {
    Log.v("Testing Confidence Levels")
    for (confidence <- Seq(25, 50, 75, 100)) {
      //      tester(s"confidence-${confidence}_ffn_bow_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"), predict = true)
      tester(s"confidence-${confidence}_ffn_w2v_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"), predict = true)
      tester(s"confidence-${confidence}_nb_w2v_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"))
      tester(s"confidence-${confidence}_nb_bow_all").test(Fetcher.by(s"confidence/nyt_mini_test_ordered_${confidence}.txt"))
    }
  }

  /** Test model on every test set matching name */
  def testBuckets(name: String, tester: Testable, criterion: Article => Double) = {
    val modelName: String = tester.fullName.split("/").last
    Log.toFile("", s"test/$name/${modelName}.txt")
    Log.toFile(s"Testing $name - ${Config.getArgs}", s"test/$name/${modelName}.txt")
    Log.toFile("", s"test/$name/${modelName}.txt")
    val testFiles: Array[File] = new File(Config.dataPath + s"nyt/$name/").listFiles
    Log.v("Test folder found: " + testFiles.map(_.getName).mkString(", "))
    val rdds: Array[(Int, RDD[Article])] = testFiles
      .map(_.getName).filter(_.contains(s"test"))
      .map(n => (n.split("_|-|\\.").filter(s => Try(s.toInt).isSuccess).last.toInt, n))
      .map { case (index, n) => (index, Fetcher.fetch(s"nyt/$name/$n")) }
      .sortBy(_._1)
    rdds.foreach { case (index, n) => {
      Log.toFile(s"${name} group: $index -  min: ${Try(n.map(criterion).min).getOrElse("N/A")} - max: ${Try(n.map(criterion).max).getOrElse("N/A")}", s"test/$name/${modelName}_labels.txt")
      tester.test(n, iteration = index)
    }
    }
  }
}

sealed trait Testable {

  import scala.collection.JavaConverters._

  val fullName: String

  def iter(test: CorpusDataset, label: String): DataSetIterator = ???

  def models(modelName: String): Map[String, NeuralModel] = ???

  def verify: Boolean = Try(models(fullName)).isSuccess

  def test(test: RDD[Article], predict: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    test.persist()
    Log.v(s"Testing ${test.count} articles ...")
    val predictedArticles: Option[RDD[Article]] = if (predict) Some(predictAll(test)) else None

    val resultFile = s"test/$fullName.txt"
    val resultFileLabels = s"test/${fullName}_labels.txt"
    val labelEvals: Seq[NeuralEvaluation] = labelBased(test, iteration)
    NeuralEvaluation.logLabelStats(labelEvals, resultFileLabels)
    NeuralEvaluation.log(labelEvals, resultFile, IPTC.topCategories, iteration, predictedArticles)
    if (shouldLogResults) predictedArticles.foreach(logResults)
    test.unpersist()
    System.gc()
  }

  def logResults(articles: RDD[Article]) = {
    articles.sortBy(_.id).foreach(a => {
      a.pred.foreach(p => {
        val folder: String = Config.dataPath + s"predictions/$fullName/" + (if (a.iptc.contains(p)) "tp" else "fp")
        Log.saveToFile(a.toStringFull, p, folder)
      })
    })
  }

  def dataset(test: RDD[Article]): CorpusDataset = {
    val tfidf = TFIDF.deserialize(fullName)
    if (fullName.contains("bow")) CorpusDataset.genBoWDataset(test, tfidf) else CorpusDataset.genW2VDataset(test, tfidf)
  }

  def predictAll(test: RDD[Article]): RDD[Article] = {
    val modelType = TFIDF.deserialize(fullName)
    NeuralPredictor.predict(test, models(fullName), modelType)
  }

  def labelBased(test: RDD[Article], iteration: Int): Seq[NeuralEvaluation] = {
    if (iteration == 0) Log.toFile(s"Testing $fullName ...", s"test/$fullName.txt")
    val testDataset: CorpusDataset = dataset(test)
    models(fullName).par.toSeq.zipWithIndex.map { case (models, i) => {
      val test = iter(testDataset, models._1).asScala.toTraversable
      val eval = NeuralEvaluation(test, models._2.network, i, models._1)
      (i, eval)
    }
    }.seq.sortBy(_._1).map(_._2)
  }
}

case class FeedforwardTester(fullName: String) extends Testable {
  lazy val ffa: Map[String, NeuralModel] = NeuralModelLoader.models(fullName)

  override def iter(test: CorpusDataset, label: String): DataSetIterator = new FeedForwardIterator(test, IPTC.topCategories.indexOf(label), 500)
  override def models(modelName: String): Map[String, NeuralModel] = ffa
}

case class RecurrentTester(fullName: String) extends Testable {
  lazy val rnn: Map[String, NeuralModel] = NeuralModelLoader.models(fullName)

  override def iter(test: CorpusDataset, label: String): DataSetIterator = new RNNIterator(test, label, 50)
  override def models(modelName: String): Map[String, NeuralModel] = rnn
}

case class NaiveBayesTester(fullName: String) extends Testable {
  lazy val nb: Map[String, NaiveBayesModel] = IPTC.topCategories.map(c => (c, MLlibModelLoader.load(fullName, IPTC.trim(c)))).toMap

  override def verify: Boolean = Try(nb).isSuccess

  override def test(articles: RDD[Article], includeExampleBased: Boolean = false, iteration: Int = 0, shouldLogResults: Boolean = false) = {
    val predicted = MlLibUtils.testMLlibModels(articles, nb, TFIDF.deserialize(fullName))

    Log.v("--- Predictions complete! ")
    MlLibUtils.evaluate(predicted, sc.broadcast(Prefs(iteration = iteration)), s"test/$fullName.txt", s"test/${fullName}_labels.txt")
    if (shouldLogResults) logResults(predicted)
    System.gc()
  }
}
