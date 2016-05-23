package no.habitats.corpus.dl4j

import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{IPTC, Log}
import no.habitats.corpus.dl4j.NeuralEvaluation.columnWidth
import no.habitats.corpus.mllib._
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.conf.layers.{DenseLayer, GravesLSTM}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.dataset.DataSet

import scala.util.Try

case class NeuralEvaluation(net: MultiLayerNetwork, testIter: TraversableOnce[DataSet], epoch: Int, label: String, neuralPrefs: Option[NeuralPrefs] = None) {
  private lazy val eval = {
    val e = new Evaluation()
    testIter.foreach(t => {
      val features = t.getFeatureMatrix
      val labels = t.getLabels
      if (t.getFeaturesMaskArray != null) {
        // timeseries eval
        val inMask = t.getFeaturesMaskArray
        val outMask = t.getLabelsMaskArray
        val predicted = net.output(features, false, inMask, outMask)
        e.evalTimeSeries(labels, predicted, outMask)
      } else {
        // matrix eval
        val predicted = net.output(features, false)
        e.eval(labels, predicted)
      }
    })
    e
  }

  private lazy val fullStats = Seq[(String, String)](
    "Category" -> f"$label%41s",
    "Epoch" -> f"${s"$epoch${neuralPrefs.map(i => "/" + (i.listener.iterCount - 1)).getOrElse("")}"}%6s",
    "TP" -> f"$tp%5d",
    "FP" -> f"$fp%5d",
    "FN" -> f"$fn%5d",
    "TN" -> f"$tn%5d",
    "Recall" -> f"${m.recall}%.3f",
    "Precision" -> f"${m.precision}%.3f",
    "Accuracy" -> f"${m.accuracy}%.3f",
    "F-score" -> f"${m.fscore}%.3f",
    "Error" -> f"${neuralPrefs.map(_.listener.average).map(a => f"$a%.10f").getOrElse("N/A")}",
    "Delta" -> f"${neuralPrefs.flatMap(i => Try(i.listener.iterationFrequency).toOption).map(a => f"$a%6d").getOrElse("N/A")}",
    "LR" -> f"${net.getLayerWiseConfigurations.getConf(0).getLayer.getLearningRate}",
    "MBS" -> f"${neuralPrefs.map(_.minibatchSize).getOrElse("N/A")}",
    "Hidden" -> f"$numHidden"
  )

  val numHidden = {
    val numLayers = net.getLayerWiseConfigurations.getConfs.size
    (0 until numLayers - 1).map(net.getLayerWiseConfigurations.getConf).map(_.getLayer).map(l => Try(l.asInstanceOf[DenseLayer].getNOut).getOrElse(l.asInstanceOf[GravesLSTM].getNOut)).mkString(", ")
  }

  lazy val statsHeader = fullStats.map(s => s"%${columnWidth(s)}s".format(s._1)).mkString("")
  lazy val stats       = fullStats.map(s => s"%${columnWidth(s)}s".format(s._2)).mkString("")
  lazy val confusion   = {
    eval.getConfusionMatrix.toCSV.split("\n")
      .map(_.split(",").zipWithIndex.map { case (k, v) => if (v == 0) f"$k%12s" else f"$k%6s" }.mkString(""))
      .mkString("\n", "\n", "")
  }

  lazy val tp: Int     = eval.truePositives.getOrDefault(1, 0)
  lazy val fp: Int     = eval.falsePositives.getOrDefault(1, 0)
  lazy val tn: Int     = eval.trueNegatives.getOrDefault(1, 0)
  lazy val fn: Int     = eval.falseNegatives.getOrDefault(1, 0)
  lazy val m : Measure = Measure(tp = tp, fp = fp, fn = fn, tn = tn)

  def log() = {
    //    Log.r2(confusion)
    Log.rr(statsHeader)
    Log.r2(stats)
  }

  def logv(i: Int) = {
    //    Log.r2(confusion)
    if (i == 0) Log.r(statsHeader, "spam.txt")
    Log.r(stats, "spam.txt")
  }
}

object NeuralEvaluation {

  def log(evals: Seq[NeuralEvaluation], cats: Seq[String], iteration: Int, predicted: Option[Seq[Article]] = None) = {
    // Macro
    val maRecall = evals.map(_.m.recall).sum / cats.size
    val maPrecision = evals.map(_.m.precision).sum / cats.size
    val maAccuracy = evals.map(_.m.accuracy).sum / cats.size
    val maFscore = evals.map(_.m.fscore).sum / cats.size

    // Micro
    val tp = evals.map(_.tp).sum
    val tn = evals.map(_.tn).sum
    val fp = evals.map(_.fp).sum
    val fn = evals.map(_.fn).sum
    val mi = Measure(tp = tp,fp = fp,fn = fn,tn = tn)

    val labelStats = Seq[(String, String)](
      "Ma.Recall" -> f"$maRecall%.3f",
      "Ma.Precision" -> f"$maPrecision%.3f",
      "Ma.Accuracy" -> f"$maAccuracy%.3f",
      "Ma.F-score" -> f"$maFscore%.3f",

      "Mi.Recall" -> f"${mi.recall}%.3f",
      "Mi.Precision" -> f"${mi.precision}%.3f",
      "Mi.Accuracy" -> f"${mi.accuracy}%.3f",
      "Mi.F-score" -> f"${mi.fscore}%.3f"
    )

    val exampleStats: Seq[(String, String)] = predicted.map(predicted => {
      val cats: Set[String] = IPTC.topCategories.toSet
      val labelMetrics = LabelMetrics(predicted)
      val exampleBased = ExampleBased(predicted, cats)
      val microAverage = MicroAverage(predicted, cats)
      val macroAverage = MacroAverage(predicted, cats)

      Seq[(String, String)](
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
        "Sub-Acc" -> f"${exampleBased.subsetAcc}%.3f",

        // Label stats
        "LCard" -> f"${labelMetrics.labelCardinality}%.3f",
        "Pred LCard" -> f"${labelMetrics.labelCardinalityPred}%.3f",
        "LDiv" -> f"${labelMetrics.labelDiversity}%.3f",
        "Pred LDiv" -> f"${labelMetrics.labelDiversityPred}%.3f"
      )
    }).getOrElse(Nil)

    Log.rr((labelStats).map(s => s"%${columnWidth(s)}s".format(s._1)).mkString(""))
    Log.r((labelStats).map(s => s"%${columnWidth(s)}s".format(s._2)).mkString(""))
    Log.rr((exampleStats).map(s => s"%${columnWidth(s)}s".format(s._1)).mkString(""))
    Log.r((exampleStats).map(s => s"%${columnWidth(s)}s".format(s._2)).mkString(""))

  }

  def columnWidth(s: (String, String)): Int = Math.max(s._1.length, s._2.toString.length) + 2
}
