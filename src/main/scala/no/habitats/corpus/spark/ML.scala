package no.habitats.corpus.spark

import no.habitats.corpus.models.Article
import no.habitats.corpus.{Config, Log, Prefs}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object ML {

  def multiLabelClassification(prefs: Broadcast[Prefs], rdd: RDD[Article]) = {
    val phrases = rdd.flatMap(_.ann.keySet).collect.distinct.sorted
    val splits = rdd.randomSplit(Array(0.8, 0.20), seed = 1L)

    val training = splits(0).map(a => (a.iptc, a.toVector(phrases)))
    training.cache
    val testing = splits(1).map(t => (t, t.toVector(phrases)))
    testing.cache

    val catModelPairs = trainModelsNaiveBayedMultiNominal(training)
    //      val catModelPairs = trainModelsSVM(training)
    Log.v("--- Training complete! ")
    val predicted = predictCategories(catModelPairs, testing)

    Log.v("--- Predictions complete! ")
    evaluate(rdd, predicted, training, phrases, prefs)
  }

  def trainModelsSVM(training: RDD[(Set[String], Vector)]): Map[String, ClassificationModel] = {
    // Run training algorithm to build the model
    val numIterations = 100
    val cats = training.flatMap(_._1).collect.toSet
    cats.map(c => {
      Log.v(s"Training ... $c ...")
      val labeledTraining = training
        .map(a => (if (a._1.contains(c)) 1 else 0, a._2))
        .map(a => LabeledPoint(a._1, a._2))
      val model = SVMWithSGD.train(labeledTraining, numIterations)
      (c, model)
    }).toMap
  }

  def trainModelsNaiveBayedMultiNominal(training: RDD[(Set[String], Vector)]): Map[String, ClassificationModel] = {
    val cats = training.flatMap(_._1).collect.toSet
    cats.map(c => {
      Log.v(s"Training ... $c ...")
      val labeledTraining = training
        .map(a => (if (a._1.contains(c)) 1 else 0, a._2))
        .map(a => LabeledPoint(a._1, a._2))
      val model = NaiveBayes.train(input = labeledTraining, lambda = 1.0, modelType = "multinomial")
      (c, model)
    }).toMap
  }

  def trainModelsNaiveBayedProbabilistic(training: RDD[(Set[String], Vector)]): Map[String, ClassificationModel] = {
    val cats = training.flatMap(_._1).collect.toSet
    cats.map(c => {
      Log.v(s"Training ... $c ...")
      val labeledTraining = training
        .map(a => (if (a._1.contains(c)) 1 else 0, a._2))
        .map(a => LabeledPoint(a._1, a._2))
      val model = NaiveBayes.train(input = labeledTraining, lambda = 1.0, modelType = "bernoulli")
      (c, model)
    }).toMap
  }

  def predictCategories(catModelPairs: Map[String, ClassificationModel], testing: RDD[(Article, Vector)], threshold: Double = 1d): RDD[Article] = {
    testing.map(t => {
      val p = catModelPairs.map(c => (c._1, c._2.predict(t._2) >= threshold)).filter(_._2).keySet
      t._1.copy(pred = p)
    })
  }

  def evaluate(rdd: RDD[Article], predicted: RDD[Article], training: RDD[(Set[String], Vector)], phrases: Seq[String], prefs: Broadcast[Prefs]) = {
    val sampleResult = predicted.take(1000).map(_.toResult).mkString("\n")
    Log.r3(sampleResult, "sample_result_" + Config.data)
    val labelCardinalityDistribution = predicted.take(2000).map(p => f"${p.iptc.size}%2d ${p.pred.size}%2d").mkString("\n")
    Log.r3(labelCardinalityDistribution, "label_cardinality_distribution_" + Config.data)

    val stats = MLStats(predicted, training, phrases, prefs)
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

case class MLStats(predicted: RDD[Article], training: RDD[(Set[String], Vector)], phrases: Seq[String], prefs: Broadcast[Prefs]) {
  predicted.cache()
  lazy val totalCats = predicted.map(_.iptc.size).sum
  lazy val totalPredictions = predicted.map(_.pred.size).sum

  val predset = predicted.collect().toSet
  val cats = training.flatMap(_._1).collect.toSet
  lazy val labelMetrics = LabelMetrics(predset)
  lazy val exampleBased = ExampleBased(predset, cats)
  lazy val microAverage = MicroAverage(predset, cats)
  lazy val macroAverage = MacroAverage(predset, cats)

  // Formatted stats
  lazy val realCategoryDistribution = predicted.flatMap(_.iptc).map((_, 1)).reduceByKey(_ + _).collect.toMap
  lazy val predictedCategoryDistribution = predicted.flatMap(_.pred).map((_, 1)).reduceByKey(_ + _).collect.toMap
  lazy val catStats = {
    macroAverage.labelStats.map(c => {
      Seq[(String, String)](
        "Cat" -> f"${c._1}%45s",
        "# Real" -> f"${realCategoryDistribution(c._1)}%5d",
        "# Pred" -> f"${predictedCategoryDistribution.getOrElse(c._1, 0)}%5d",
        "TP" -> f"${c._2.tp}%5d",
        "FP" -> f"${c._2.fp}%5d",
        "FN" -> f"${c._2.fn}%5d",
        "TN" -> f"${c._2.tn}%5d",
        "Recall" -> f"${c._2.recall}%.3f",
        "Precision" -> f"${c._2.precision}%.3f",
        "Accuracy" -> f"${c._2.accuracy}%.3f",
        "F-score" -> f"${c._2.fscore}%.3f")
    }).toSeq
  }
  lazy val stats = Seq[(String, String)](
    "#" -> f"${prefs.value.iteration}%3d",

    // Dynamic prefs
    "TFT" -> f"${prefs.value.termFrequencyThreshold}",

    "S-Only" -> f"${prefs.value.salientOnly}",
    "S-Weight" -> (if (!prefs.value.salientOnly) f"${prefs.value.salience}%.1f" else " "),

    "WD-Only" -> f"${prefs.value.wikiDataOnly}",
    "WD-Broad" -> f"${prefs.value.wikiDataIncludeBroad}",
    "WD-BroadO" -> (if (prefs.value.wikiDataIncludeBroad) f"${prefs.value.wikiDataBroadOnly}" else " "),
    "WD-BroadS" -> (if (prefs.value.wikiDataIncludeBroad) f"${prefs.value.wikiDataBroadSalience}%.1f" else " "),

    // Data stats
    "Articles" -> f"${training.count + predicted.count}",
    "Train/Test" -> f"${training.count + "/" + predicted.count}%11s",
    "Categories" -> f"${totalCats.toInt}",
    "Annotations" -> f"${phrases.size}",
    "Pred/True" -> f"${totalPredictions / totalCats}%.3f",
    "LCard" -> f"${labelMetrics.labelCardinality}%.3f",
    "Pred LCard" -> f"${labelMetrics.labelCardinalityPred}%.3f",
    "LDiv" -> f"${labelMetrics.labelDiversity}%.3f",
    "Pred LDiv" -> f"${labelMetrics.labelDiversityPred}%.3f",
    "H-Loss" -> f"${exampleBased.hloss}%.3f",
    "Sub-Acc" -> f"${exampleBased.subsetAcc}%.3f",

    // Label-based
    "Ma.Recall" -> f"${macroAverage.recall}%.3f",
    "Ma.Precision" -> f"${macroAverage.precision}%.3f",
    "Ma.Accuracy" -> f"${macroAverage.accuracy}%.3f",
    "Ma.F-score" -> f"${macroAverage.fscore}%.3f",

    "Mi.Recall" -> f"${microAverage.recall}%.3f",
    "Mi.Precision" -> f"${microAverage.precision}%.3f",
    "Mi.Accuracy" -> f"${microAverage.accuracy}%.3f",
    "Mi.F-score" -> f"${microAverage.fscore}%.3f",

    // Example-based
    "Ex.Recall" -> f"${exampleBased.recall}%.3f",
    "Ex.Precision" -> f"${exampleBased.precision}%.3f",
    "Ex.Accuracy" -> f"${exampleBased.accuracy}%.3f",
    "Ex.F-score" -> f"${exampleBased.fscore}%.3f"
  )
}

case class LabelMetrics(predicted: Set[Article]) {
  lazy val p = predicted.size.toDouble
  lazy val labelCardinality = predicted.toList.map(_.iptc.size).sum / p
  lazy val labelDiversity = predicted.map(_.iptc).size / p
  lazy val labelCardinalityPred = predicted.toList.map(_.pred.size).sum / p
  lazy val labelDiversityPred = predicted.map(_.pred).size / p
}

// Example-based metrics
case class ExampleBased(predicted: Set[Article], cats: Set[String]) {
  lazy val p = predicted.size.toDouble
  lazy val subsetAcc = predicted.toList.count(p => p.pred == p.iptc) / p
  lazy val hloss = predicted.toList.map(p => (p.iptc.union(p.pred) -- p.iptc.intersect(p.pred)).size.toDouble / p.iptc.union(p.pred).size).sum / p

  lazy val precision = predicted.toList.filter(_.pred.nonEmpty).map(p => p.iptc.intersect(p.pred).size.toDouble / p.pred.size).sum / p
  lazy val recall = predicted.toList.map(p => p.iptc.intersect(p.pred).size.toDouble / p.iptc.size).sum / p
  lazy val accuracy = predicted.toList.map(p => p.iptc.intersect(p.pred).size.toDouble / p.iptc.union(p.pred).size).sum / p

  lazy val fscore = 2 * (precision * recall) / (precision + recall)
}

// Label-based metrics
case class Measure(tp: Int, fp: Int, fn: Int, tn: Int) {
  val recall = tp.toDouble / (tp + fn)
  val precision = tp.toDouble / (tp + fp + 0.00001)
  val accuracy = (tp + tn).toDouble / (tp + fp + fn + tn)
  val fscore = (2 * tp).toDouble / (2 * tp + fp + fn)
}

case class LabelResult(category: String, tp: Int, fp: Int, fn: Int, tn: Int) {
  lazy val m: Measure = Measure(tp = tp, fp = fp, fn = fn, tn = tn)
  val recall = m.recall
  val precision = m.precision
  val accuracy = m.accuracy
  val fscore = m.fscore
}

case class MicroAverage(predicted: Set[Article], cats: Set[String]) {
  val tp = cats.toList.map(c => predicted.count(p => p.iptc.contains(c) && p.pred.contains(c))).sum
  val fp = cats.toList.map(c => predicted.count(p => !p.iptc.contains(c) && p.pred.contains(c))).sum
  val fn = cats.toList.map(c => predicted.count(p => p.iptc.contains(c) && !p.pred.contains(c))).sum
  val tn = cats.toList.map(c => predicted.count(p => !p.iptc.contains(c) && !p.pred.contains(c))).sum
  lazy val m: Measure = Measure(tp = tp, fp = fp, fn = fn, tn = tn)

  val recall = m.recall
  val precision = m.precision
  val accuracy = m.accuracy
  val fscore = m.fscore
}

case class MacroAverage(predicted: Set[Article], cats: Set[String]) {
  val labelStats: Map[String, LabelResult] = {
    val predCats = predicted.flatMap(_.iptc)
    val l = for {
      c <- predCats
      tp = predicted.count(p => p.iptc.contains(c) && p.pred.contains(c))
      fp = predicted.count(p => !p.iptc.contains(c) && p.pred.contains(c))
      fn = predicted.count(p => p.iptc.contains(c) && !p.pred.contains(c))
      tn = predicted.count(p => !p.iptc.contains(c) && !p.pred.contains(c))
    } yield (c, LabelResult(category = c, tp = tp, fp = fp, fn = fn, tn = tn))
    l.toMap
  }

  def tp(c: String): Int = labelStats(c).tp

  def fp(c: String): Int = labelStats(c).fp

  def fn(c: String): Int = labelStats(c).fn

  def tn(c: String): Int = labelStats(c).tn

  val recall = labelStats.values.map(_.recall).sum / labelStats.size
  val precision = labelStats.values.map(_.precision).sum / labelStats.size
  val accuracy = labelStats.values.map(_.accuracy).sum / labelStats.size
  val fscore = labelStats.values.map(_.fscore).sum / labelStats.size
}
