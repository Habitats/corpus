package no.habitats.corpus.mllib

import no.habitats.corpus.common.models.Article
import org.apache.spark.rdd.RDD

case class MLStats(predicted: RDD[Article], cats: Set[String]) {
  lazy val totalCats       : Double = predicted.map(_.iptc.size).sum
  lazy val totalPredictions: Double = predicted.map(_.pred.size).sum

  lazy val labelMetrics: LabelMetrics = LabelMetrics(predicted)
  lazy val exampleBased: ExampleBased = ExampleBased(predicted, cats)
  lazy val microAverage: MicroAverage = MicroAverage(predicted, cats)
  lazy val macroAverage: MacroAverage = MacroAverage(predicted, cats)

  // Formatted stats

  lazy val catStats: Seq[Seq[(String, String)]] = {
    macroAverage.labelStats.toSeq.sortBy(_._1).map(c => {
      Seq[(String, String)](
        "Cat" -> f"${c._1}%45s",
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
    //    "Categories" -> f"${totalCats.toInt}",
    //    "Pred/True" -> f"${totalPredictions / totalCats}%.3f",

    // Example-based
    //    "Ex.Recall" -> f"${exampleBased.recall}%.3f",
    //    "Ex.Precision" -> f"${exampleBased.precision}%.3f",
    //    "Ex.Accuracy" -> f"${exampleBased.accuracy}%.3f",
    //    "Ex.F-score" -> f"${exampleBased.fscore}%.3f",

    // Label-based
    "Ma.Recall" -> f"${macroAverage.recall}%.3f",
    "Ma.Precision" -> f"${macroAverage.precision}%.3f",
    "Ma.Accuracy" -> f"${macroAverage.accuracy}%.3f",
    "Ma.F-score" -> f"${macroAverage.fscore}%.3f",

    "Mi.Recall" -> f"${microAverage.recall}%.3f",
    "Mi.Precision" -> f"${microAverage.precision}%.3f",
    "Mi.Accuracy" -> f"${microAverage.accuracy}%.3f",
    "Mi.F-score" -> f"${microAverage.fscore}%.3f",

    "H-Loss" -> f"${exampleBased.hloss}%.3f",
    "Sub-Acc" -> f"${exampleBased.subsetAcc}%.3f",

    // Label stats
    "LCard" -> f"${labelMetrics.labelCardinality}%.3f",
    "Pred LCard" -> f"${labelMetrics.labelCardinalityPred}%.3f",
    "LDiv" -> f"${labelMetrics.labelDiversity}%.3f",
    "Pred LDiv" -> f"${labelMetrics.labelDiversityPred}%.3f"
  )
}

case class LabelMetrics(predicted: RDD[Article]) {
  val p = predicted.count.toDouble
  lazy val labelCardinality     = predicted.map(_.iptc.size).sum / p
  lazy val labelDiversity       = predicted.map(_.iptc).count / p
  lazy val labelCardinalityPred = predicted.map(_.pred.size).sum / p
  lazy val labelDiversityPred   = predicted.map(_.pred).count / p
}

// Example-based metrics
case class ExampleBased(predicted: RDD[Article], cats: Set[String]) {
  val p = predicted.count.toDouble
  lazy val subsetAcc = predicted.filter(p => p.pred == p.iptc).count / p
  lazy val hloss     = predicted.map(p => (p.iptc.union(p.pred) -- p.iptc.intersect(p.pred)).size.toDouble / p.iptc.union(p.pred).size).sum / p
  lazy val precision = predicted.filter(_.pred.nonEmpty).map(p => p.iptc.intersect(p.pred).size.toDouble / p.pred.size).sum / p
  lazy val recall    = predicted.map(p => p.iptc.intersect(p.pred).size.toDouble / p.iptc.size).sum / p
  lazy val accuracy  = predicted.map(p => p.iptc.intersect(p.pred).size.toDouble / p.iptc.union(p.pred).size).sum / p
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

case class MacroAverage(predicted: RDD[Article], cats: Set[String]) {
  lazy val labelStats: Map[String, LabelResult] = {
    val l = for {
      c <- cats
      tp = predicted.filter(p => p.iptc.contains(c) && p.pred.contains(c)).count.toInt
      fp = predicted.filter(p => !p.iptc.contains(c) && p.pred.contains(c)).count.toInt
      fn = predicted.filter(p => p.iptc.contains(c) && !p.pred.contains(c)).count.toInt
      tn = predicted.filter(p => !p.iptc.contains(c) && !p.pred.contains(c)).count.toInt
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

case class MicroAverage(predicted: RDD[Article], cats: Set[String]) {
  lazy val tp         = cats.toList.map(c => predicted.filter(p => p.iptc.contains(c) && p.pred.contains(c)).count).sum.toInt
  lazy val fp         = cats.toList.map(c => predicted.filter(p => !p.iptc.contains(c) && p.pred.contains(c)).count).sum.toInt
  lazy val fn         = cats.toList.map(c => predicted.filter(p => p.iptc.contains(c) && !p.pred.contains(c)).count).sum.toInt
  lazy val tn         = cats.toList.map(c => predicted.filter(p => !p.iptc.contains(c) && !p.pred.contains(c)).count).sum.toInt
  lazy val m: Measure = Measure(tp = tp, fp = fp, fn = fn, tn = tn)

  lazy val recall    = m.recall
  lazy val precision = m.precision
  lazy val accuracy  = m.accuracy
  lazy val fscore    = m.fscore
}
