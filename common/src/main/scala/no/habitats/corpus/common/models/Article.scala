package no.habitats.corpus.common.models

import no.habitats.corpus.common.{IPTC, W2VLoader}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.ops.transforms.Transforms

case class Article(id: String,
                   hl: String = "",
                   body: String = "",
                   wc: Int = -1,
                   date: Option[String] = None,
                   iptc: Set[String] = Set(),
                   url: Option[String] = None,
                   desc: Set[String] = Set(),
                   pred: Set[String] = Set(),
                   ann: Map[String, Annotation] = Map()) extends JSonable {

  override def toString: String = {
    val iptcstr = iptc.mkString(", ")
    val annstr = ann.values.map(_.phrase).mkString(", ")
    f"$id - $hl >> TAGS >> $iptcstr >> PHRASES >> $annstr"
  }

  def filterAnnotation(f: Annotation => Boolean): Article = copy(ann = ann.filter { case (id, an) => f(an) })

  def toStringFull: String = {
    val iptcstr = iptc.mkString(", ")
    val annstr = ann.values.toSeq.sortBy(_.tfIdf).mkString("\n\t\t")
    val dstr = desc.mkString(", ")
    val predstr = pred.mkString(", ")
    f"$id - $hl - $wc - $date - $url\n\tDescriptors: $dstr\n\tIPTC:         $iptcstr\n\tPredictions: $predstr\n\tAnnotations (${ann.size}):\n\t\t$annstr\n"
  }

  def toResult: String = {
    val iptcstr = iptc.mkString(" - ")
    val predstr = pred.mkString(" - ")
    f"$id - $hl - $url - IPTC >> $iptcstr >> PREDICTION >> $predstr"
  }

  def toVector(phrases: Array[String]): Vector = {
    toVectorSparse(phrases)
  }

  def toMinimal: Article = copy(body = "", desc = Set(), date = None, hl = "")

  def toDocumentVector: Vector = {
    val all = ann.map(_._2.fb).map(W2VLoader.fromId).filter(_.isDefined).map(_.get)
    // Min: -0.15568943321704865
    // Max:  0.15866121649742126
    val min = -0.16
    val max = 0.16
    val squashed: INDArray = all.reduce(_.add(_)).div(all.size)
    val normal = squashed.sub(min).div(max - min)
    val binary = Transforms.round(normal)
    MLLibUtil.toVector(binary)
  }

  def toVectorDense(phrases: Array[String]): Vector = {
    val row = phrases.map(w => if (ann.contains(w)) ann(w).tfIdf else 0).toArray
    Vectors.dense(row)
  }

  def toVectorSparse(phrases: Array[String]): Vector = {
    val values: Seq[(Int, Double)] = for {
      i <- phrases.indices
      w = phrases(i) if ann.contains(w) && ann(w).tfIdf > 0
    } yield (i, ann(w).tfIdf)
    Vectors.sparse(phrases.size, values)
  }

  def addIptc(broad: Boolean): Article = {
    copy(iptc = if (broad) IPTC.toBroad(desc, 0) else IPTC.toIptc(desc))
  }

  def toND4JDocumentVector: INDArray = {
    val vectors = ann.values.map(_.fb).map(W2VLoader.fromId).map(_.get)
    val combined = vectors.reduce(_.add(_)).div(vectors.size)
    combined
  }
}


