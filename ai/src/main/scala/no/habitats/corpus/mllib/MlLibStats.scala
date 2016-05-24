package no.habitats.corpus.mllib

import no.habitats.corpus.common.models.Article
import org.apache.spark.rdd.RDD

case class MLStats(predicted: RDD[Article], cats: Set[String]) {
//  predicted.cache()
  lazy val totalCats       : Double = predicted.map(_.iptc.size).sum
  lazy val totalPredictions: Double = predicted.map(_.pred.size).sum

  val predset = predicted.collect()
  lazy val labelMetrics: LabelMetrics = LabelMetrics(predset)
  lazy val exampleBased: ExampleBased = ExampleBased(predset, cats)
  lazy val microAverage: MicroAverage = MicroAverage(predset, cats)
  lazy val macroAverage: MacroAverage = MacroAverage(predset, cats)

  // Formatted stats
  lazy val realCategoryDistribution     : Map[String, Int] = predicted.flatMap(_.iptc).map((_, 1)).reduceByKey(_ + _).collect.toMap
  lazy val predictedCategoryDistribution: Map[String, Int] = predicted.flatMap(_.pred).map((_, 1)).reduceByKey(_ + _).collect.toMap

  lazy val catStats: Seq[Seq[(String, String)]] = {
    macroAverage.labelStats.toSeq.sortBy(_._1).map(c => {
      Seq[(String, String)](
        "Cat" -> f"${c._1}%45s",
//        "# Real" -> f"${realCategoryDistribution.getOrElse(c._1, 0)}%5d",
//        "# Pred" -> f"${predictedCategoryDistribution.getOrElse(c._1, 0)}%5d",
        "TP" -> f"${c._2.tp}%5d",
        "FP" -> f"${c._2.fp}%5d",
        "FN" -> f"${c._2.fn}%5d",
        "TN" -> f"${c._2.tn}%5d",
        "Recall" -> f"${c._2.recall}%.3f",
        "Precision" -> f"${c._2.precision}%.3f",
        "Accuracy" -> f"${c._2.accuracy}%.3f",
        "F-score" -> f"${c._2.fscore}%.3f")
    })
  }

  lazy val stats = Seq[(String, String)](
    // Data stats
    "Categories" -> f"${totalCats.toInt}",
    "Pred/True" -> f"${totalPredictions / totalCats}%.3f",

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
    "Ex.F-score" -> f"${exampleBased.fscore}%.3f",
    "H-Loss" -> f"${exampleBased.hloss}%.3f",
    "Sub-Acc" -> f"${exampleBased.subsetAcc}%.3f"

    // Label stats
//    "LCard" -> f"${labelMetrics.labelCardinality}%.3f",
//    "Pred LCard" -> f"${labelMetrics.labelCardinalityPred}%.3f",
//    "LDiv" -> f"${labelMetrics.labelDiversity}%.3f",
//    "Pred LDiv" -> f"${labelMetrics.labelDiversityPred}%.3f"
  )
}

case class LabelMetrics(predicted: Seq[Article]) {
  lazy val p                    = predicted.size.toDouble
  lazy val labelCardinality     = predicted.toList.map(_.iptc.size).sum / p
  lazy val labelDiversity       = predicted.map(_.iptc).size / p
  lazy val labelCardinalityPred = predicted.toList.map(_.pred.size).sum / p
  lazy val labelDiversityPred   = predicted.map(_.pred).size / p
}

// Example-based metrics
case class ExampleBased(predicted: Seq[Article], cats: Set[String]) {
  lazy val p         = predicted.size.toDouble
  lazy val subsetAcc = predicted.toList.count(p => p.pred == p.iptc) / p
  lazy val hloss     = predicted.toList.map(p => (p.iptc.union(p.pred) -- p.iptc.intersect(p.pred)).size.toDouble / p.iptc.union(p.pred).size).sum / p
  lazy val precision = predicted.toList.filter(_.pred.nonEmpty).map(p => p.iptc.intersect(p.pred).size.toDouble / p.pred.size).sum / p
  lazy val recall    = predicted.toList.map(p => p.iptc.intersect(p.pred).size.toDouble / p.iptc.size).sum / p
  lazy val accuracy  = predicted.toList.map(p => p.iptc.intersect(p.pred).size.toDouble / p.iptc.union(p.pred).size).sum / p
  lazy val fscore    = 2 * (precision * recall) / (precision + recall)
}

// Label-based metrics
case class Measure(tp: Int, fp: Int, fn: Int, tn: Int) {
  lazy val recall    = if (fn == 0) 1 else tp.toDouble / (tp + fn)
  lazy val precision = if (fp == 0) 1 else tp.toDouble / (tp + fp)
  lazy val accuracy  = (tp + tn).toDouble / (tp + fp + fn + tn)
  lazy val fscore    = 2 * (precision * recall) / (precision + recall + 0.0001)
  override def toString = s"TP: $tp - FP: $fp - FN: $fn - TN: $tn"
}

case class LabelResult(category: String, tp: Int, fp: Int, fn: Int, tn: Int) {
  lazy val m: Measure = Measure(tp = tp, fp = fp, fn = fn, tn = tn)
  lazy val recall     = m.recall
  lazy val precision  = m.precision
  lazy val accuracy   = m.accuracy
  lazy val fscore     = m.fscore
}

case class MacroAverage(predicted: Seq[Article], cats: Set[String]) {
  lazy val labelStats: Map[String, LabelResult] = {
    val l = for {
      c <- cats
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

  lazy val recall    = labelStats.values.map(_.recall).sum / labelStats.size
  lazy val precision = labelStats.values.map(_.precision).sum / labelStats.size
  lazy val accuracy  = labelStats.values.map(_.accuracy).sum / labelStats.size
  lazy val fscore    = labelStats.values.map(_.fscore).sum / labelStats.size
}

case class MicroAverage(predicted: Seq[Article], cats: Set[String]) {
  lazy val tp         = cats.toList.map(c => predicted.count(p => p.iptc.contains(c) && p.pred.contains(c))).sum
  lazy val fp         = cats.toList.map(c => predicted.count(p => !p.iptc.contains(c) && p.pred.contains(c))).sum
  lazy val fn         = cats.toList.map(c => predicted.count(p => p.iptc.contains(c) && !p.pred.contains(c))).sum
  lazy val tn         = cats.toList.map(c => predicted.count(p => !p.iptc.contains(c) && !p.pred.contains(c))).sum
  lazy val m: Measure = Measure(tp = tp, fp = fp, fn = fn, tn = tn)

  lazy val recall    = m.recall
  lazy val precision = m.precision
  lazy val accuracy  = m.accuracy
  lazy val fscore    = m.fscore
}
