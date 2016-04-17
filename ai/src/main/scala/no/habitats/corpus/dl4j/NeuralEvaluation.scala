package no.habitats.corpus.dl4j

import no.habitats.corpus.common.Log
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.collection.JavaConverters._

case class NeuralEvaluation(net: MultiLayerNetwork, testIter: DataSetIterator, epoch: Int) {
  private val eval = {
    val e = new Evaluation()
    testIter.asScala.toList.foreach(t => {
      val features = t.getFeatureMatrix
      val labels = t.getLabels
      val inMask = t.getFeaturesMaskArray
      val outMask = t.getLabelsMaskArray
      val predicted = net.output(features, false, inMask, outMask)
      e.evalTimeSeries(labels, predicted, outMask)
    })
    e
  }

  private val fullStats = Seq[(String, String)](
    "Epoch" -> f"$epoch%5d",
    "TP" -> f"${eval.truePositives.get(1)}%5d",
    "FP" -> f"${eval.falsePositives.get(1)}%5d",
    "FN" -> f"${eval.falseNegatives.get(1)}%5d",
    "TN" -> f"${eval.trueNegatives.get(1)}%5d",
    "Recall" -> f"${eval.recall}%.3f",
    "Precision" -> f"${eval.precision}%.3f",
    "Accuracy" -> f"${eval.accuracy}%.3f",
    "F-score" -> f"${eval.f1}%.3f"
  )

  val statsHeader = fullStats.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 2}s").format(s._1)).mkString("")
  val stats       = fullStats.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 2}s").format(s._2)).mkString("")
  val confusion   = {
    eval.getConfusionMatrix.toCSV.split("\n")
      .map(_.split(",").zipWithIndex.map { case (k, v) => if (v == 0) f"$k%12s" else f"$k%6s" }.mkString(""))
      .mkString("\n", "\n", "")
  }

  def log() = {
    Log.r2(confusion)
    if (epoch == 0) Log.r(statsHeader)
    Log.r(stats)
  }
}
