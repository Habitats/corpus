package no.habitats.corpus.dl4j

import no.habitats.corpus.npl.IPTC
import org.deeplearning4j.eval.ConfusionMatrix

case class CorpusConfusion {
  val classes: Map[Int, String] = IPTC.topCategories.zipWithIndex.map { case (k, v) => v -> k }.toMap
  private val confusion = new scala.collection.mutable.HashMap[String, SingleModelEval]

//  def addConfusion(confusionMatrix: ConfusionMatrix[Int], category: String) = {
//    val tp = confusionMatrix.getCount(1,1)
//    val fp = confusionMatrix.getCount(0,1)
//    val fn = confusionMatrix.getCount(1,0)
//    val tn = confusionMatrix.getCount(0,0)
//    val eval = SingleModelEval(tp, fp, fn, tn)
//    confusion.put(category, eval)
//  }
}

case class SingleModelEval(tp: Int, fp: Int, fn: Int, tn: Int)
