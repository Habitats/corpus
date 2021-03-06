package no.habitats.corpus.mllib

import no.habitats.corpus.common._
import no.habitats.corpus.common.models.{Article, CorpusDataset}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object MlLibUtils {

  def multiLabelClassification(c: String, train: RDD[Article], test: RDD[Article], tfidf: TFIDF): NaiveBayesModel = {
    val training: RDD[(Set[String], Vector)] = train.filter(_.ann.exists(an => tfidf.contains(an._2.id))).map(a => (a.iptc, CorpusDataset.toVector(tfidf, a)))
    trainModelsNaiveBayedMultiNominal(training, c)
  }

  def trainModelsNaiveBayedMultiNominal(training: RDD[(Set[String], Vector)], c: String): NaiveBayesModel = {
    Log.v(s"Training ... $c ...")
    val labeledTraining: RDD[LabeledPoint] = training
      .map(a => (if (a._1.contains(c)) 1 else 0, a._2))
      .map(a => LabeledPoint(a._1, a._2))
    val model = NaiveBayes.train(input = labeledTraining, lambda = 1.0, modelType = "multinomial")
    model
  }

  def testMLlibModels(test: RDD[Article], catModelPairs: Map[String, NaiveBayesModel], tfidf: TFIDF): RDD[Article] = {
    if (tfidf.name.contains("w2v")) W2VLoader.preload()
    val testing: RDD[(Article, Vector)] = test.filter(_.ann.exists(an => tfidf.contains(an._2.id))).map(a => (a, CorpusDataset.toVector(tfidf, a)))
    //    Log.v("Max:" + testing.map(_._2.toArray.max).max)
    //    Log.v("Min: " + testing.map(_._2.toArray.min).min)
    predictCategories(catModelPairs, testing)
  }

  def predictCategories(catModelPairs: Map[String, ClassificationModel], testing: RDD[(Article, Vector)], threshold: Double = 1d): RDD[Article] = {
    testing.map(t => {
      val p = catModelPairs.map(c => (c._1, c._2.predict(t._2) >= threshold)).filter(_._2).keySet
      t._1.copy(pred = p)
    })
  }

  def evaluate(predicted: RDD[Article], prefs: Broadcast[Prefs], resultFile: String, resultFileLabels: String) = {
    predicted.cache()
    //    val sampleResult = predicted.map(_.toResult).reduce(_ + "\n" + _)
    //    Log.saveToFile(sampleResult, Config.dataPath + s"stats/sample_result_${Config.count}.txt")
    //    val labelCardinalityDistribution = predicted.map(p => f"${p.iptc.size}%2d ${p.pred.size}%2d").reduce(_ + "\n" + _)
    //    Log.saveToFile(labelCardinalityDistribution, Config.dataPath + s"stats/label_cardinality_distribution_${Config.count}.txt")

    val cats = IPTC.topCategories.toSet
    val stats = MLStats(predicted, cats)
    if (stats.catStats.nonEmpty) {
      val catHeader = stats.catStats.head.map(s => formatHeader(s)).mkString(f"${prefs.value.iteration}%3d# Category stats:\n", "", "\n")
      val catStats: Seq[String] = stats.catStats.map(c => c.map(s => formatColumn(s)).mkString(""))
      Log.toFile(catStats.mkString(catHeader, "\n", "\n"), resultFileLabels)
    }

    if (prefs.value.iteration == 0) {
      val accumulatedHeaders: String = stats.stats.map(s => formatHeader(s)).mkString("")
      Log.toFile(accumulatedHeaders, resultFile)
    }
    val accumulatedStats: String = stats.stats.map(s => formatColumn(s)).mkString("")
    Log.toFile(accumulatedStats, resultFile)
    predicted.unpersist()
  }
  def formatHeader(s: (String, String)): String = (s"%${columnWidth(s)}s").format(s._1)
  def formatColumn(s: (String, String)): String = (s"%${columnWidth(s)}s").format(s._2)
  def columnWidth(s: (String, String)): Int = Math.max(s._1.length, s._2.toString.length) + 2
}


