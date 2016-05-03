package no.habitats.corpus.dl4j

import no.habitats.corpus.common.Log
import no.habitats.corpus.dl4j.NeuralEvaluation.columnWidth
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.conf.layers.{DenseLayer, GravesLSTM}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.collection.JavaConverters._
import scala.util.Try

case class NeuralEvaluation(net: MultiLayerNetwork, testIter: DataSetIterator, epoch: Int, label: String) {
  private lazy val eval = {
    val e = new Evaluation()
    testIter.asScala.foreach(t => {
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
    "Error" -> f"${net.score}%.10f",
    "LR" -> f"${net.getLayerWiseConfigurations.getConf(0).getLayer.getLearningRate}",
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

  lazy val precision: Double = eval.precision
  lazy val recall   : Double = eval.recall
  lazy val fscore   : Double = eval.f1
  lazy val accuracy : Double = eval.accuracy
  lazy val tp       : Int    = eval.truePositives.get(1)
  lazy val fp       : Int    = eval.falsePositives.get(1)
  lazy val tn       : Int    = eval.trueNegatives.get(1)
  lazy val fn       : Int    = eval.falseNegatives.get(1)

  def log() = {
    //    Log.r2(confusion)
    if (epoch == 0) Log.r(statsHeader)
    Log.r(stats)
  }
}

object NeuralEvaluation {

  def log(evals: Set[NeuralEvaluation], cats: Seq[String]) = {
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
    Log.r(stats.map(s => s"%${columnWidth(s)}s".format(s._1)).mkString(""))
    Log.r(stats.map(s => s"%${columnWidth(s)}s".format(s._2)).mkString(""))

  }

  def columnWidth(s: (String, String)): Int = Math.max(s._1.length, s._2.toString.length) + 2
}
