package no.habitats.corpus.dl4j

import no.habitats.corpus.common.Log
import no.habitats.corpus.dl4j.NeuralEvaluation.columnWidth
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
    "Epoch" -> f"$epoch%5d",
    "TP" -> f"$tp%5d",
    "FP" -> f"$fp%5d",
    "FN" -> f"$fn%5d",
    "TN" -> f"$tn%5d",
    "Recall" -> f"$recall%.3f",
    "Precision" -> f"$precision%.3f",
    "Accuracy" -> f"$accuracy%.3f",
    "F-score" -> f"$fscore%.3f",
    "Error" -> f"${neuralPrefs.map(_.listener.average).map(a => f"$a%.10f").getOrElse("N/A")}",
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

  lazy val precision: Double = Try(eval.precision).getOrElse(0)
  lazy val recall   : Double = Try(eval.recall).getOrElse(0)
  lazy val fscore   : Double = Try(eval.f1).getOrElse(0)
  lazy val accuracy : Double = Try(eval.accuracy).getOrElse(0)
  lazy val tp       : Int    = Try(eval.truePositives.get(1).toInt).getOrElse(0)
  lazy val fp       : Int    = Try(eval.falsePositives.get(1).toInt).getOrElse(0)
  lazy val tn       : Int    = Try(eval.trueNegatives.get(1).toInt).getOrElse(0)
  lazy val fn       : Int    = Try(eval.falseNegatives.get(1).toInt).getOrElse(0)

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

  def log(evals: Set[NeuralEvaluation], cats: Seq[String], iteration: Int) = {
    // Macro
    val maRecall = evals.map(_.recall).sum / cats.size
    val maPrecision = evals.map(_.precision).sum / cats.size
    val maAccuracy = evals.map(_.accuracy).sum / cats.size
    val maFscore = evals.map(_.fscore).sum / cats.size

    // Micro
    val tp = evals.map(_.tp).sum
    val tn = evals.map(_.tn).sum
    val fp = evals.map(_.fp).sum
    val fn = evals.map(_.fn).sum
    val miRecall = tp.toDouble / (tp + fn)
    val miPrecision = tp.toDouble / (tp + fp + 0.00001)
    val miAccuracy = (tp + tn).toDouble / (tp + fp + fn + tn)
    val miFscore = (2 * tp).toDouble / (2 * tp + fp + fn)

    val stats = Seq[(String, String)](
      "Ma.Recall" -> f"$maRecall%.3f",
      "Ma.Precision" -> f"$maPrecision%.3f",
      "Ma.Accuracy" -> f"$maAccuracy%.3f",
      "Ma.F-score" -> f"$maFscore%.3f",

      "Mi.Recall" -> f"$miRecall%.3f",
      "Mi.Precision" -> f"$miPrecision%.3f",
      "Mi.Accuracy" -> f"$miAccuracy%.3f",
      "Mi.F-score" -> f"$miFscore%.3f"
    )
    Log.rr(stats.map(s => s"%${columnWidth(s)}s".format(s._1)).mkString(""))
    Log.r(stats.map(s => s"%${columnWidth(s)}s".format(s._2)).mkString(""))

  }

  def columnWidth(s: (String, String)): Int = Math.max(s._1.length, s._2.toString.length) + 2
}
