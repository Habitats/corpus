package no.habitats.corpus.spark

import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.models.Article
import no.habitats.corpus.npl.IPTC
import no.habitats.corpus.{MLStats, Prefs}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object ML {

  def multiLabelClassification(prefs: Broadcast[Prefs], train: RDD[Article], test: RDD[Article]): Map[String, NaiveBayesModel] = {
    val phrases = (train ++ test).flatMap(_.ann.keySet).collect.distinct.sorted

    val training = train.map(a => (a.iptc, a.toVector(phrases)))
    training.cache
    val testing = test.map(t => (t, t.toVector(phrases)))
    testing.cache

    val catModelPairs = trainModelsNaiveBayedMultiNominal(training, prefs.value.categories)
    //      val catModelPairs = trainModelsSVM(training)
    Log.v("--- Training complete! ")
    val predicted = predictCategories(catModelPairs, testing)

    Log.v("--- Predictions complete! ")
    evaluate( predicted, training, phrases, prefs)
    catModelPairs
  }

  def trainModelsNaiveBayedMultiNominal(training: RDD[(Set[String], Vector)], cats: Seq[String]): Map[String, NaiveBayesModel] = {
    cats.map(c => {
      Log.v(s"Training ... $c ...")
      val labeledTraining = training
        .map(a => (if (a._1.contains(c)) 1 else 0, a._2))
        .map(a => LabeledPoint(a._1, a._2))
      val model = NaiveBayes.train(input = labeledTraining, lambda = 1.0, modelType = "multinomial")
      (c, model)
    }).toMap
  }

  def predictCategories(catModelPairs: Map[String, ClassificationModel], testing: RDD[(Article, Vector)], threshold: Double = 1d): RDD[Article] = {
    testing.map(t => {
      val p = catModelPairs.map(c => (c._1, c._2.predict(t._2) >= threshold)).filter(_._2).keySet
      t._1.copy(pred = p)
    })
  }

  def evaluate( predicted: RDD[Article], training: RDD[(Set[String], Vector)], phrases: Seq[String], prefs: Broadcast[Prefs]) = {
    val sampleResult = predicted.map(_.toResult).reduce(_ + "\n" + _)
    Log.toFile(sampleResult, s"stats/sample_result_${Config.count}.txt")
    val labelCardinalityDistribution = predicted.map(p => f"${p.iptc.size}%2d ${p.pred.size}%2d").reduce(_ + "\n" + _)
    Log.toFile(labelCardinalityDistribution, s"stats/label_cardinality_distribution_${Config.count}.txt")

    val cats = training.flatMap(_._1).distinct.collect.toSet
    val stats = MLStats(predicted, training.count.toInt, cats, phrases, prefs)
    if (stats.catStats.nonEmpty) {
      val catHeader = stats.catStats.head.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 3}s").format(s._1)).mkString(f"${prefs.value.iteration}%3d# Category stats:\n", "", "\n")
      Log.r2(stats.catStats.map(c => c.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 3}s").format(s._2)).mkString("")).mkString(catHeader, "\n", "\n"))
    }

    if (prefs.value.iteration == 0) {
      Log.r(stats.stats.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 2}s").format(s._1)).mkString(""))
    }
    Log.r(stats.stats.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 2}s").format(s._2)).mkString(""))
  }
}


