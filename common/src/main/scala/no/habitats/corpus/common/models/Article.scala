package no.habitats.corpus.common.models

import no.habitats.corpus.common.{IPTC, W2VLoader}
import org.apache.spark.mllib.linalg.Vector
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray

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
    val annstr = ann.values.toSeq.sortBy(-_.tfIdf).mkString("\n\t\t")
    val dstr = desc.mkString(", ")
    val predstr = pred.mkString(", ")
    f"$id - $hl - $wc - ${date.getOrElse("")} - ${url.getOrElse("")}\n\tDescriptors: $dstr\n\tIPTC:        $iptcstr\n\tPredictions: $predstr\n\tAnnotations (${ann.size}):\n\t\t$annstr\n"
  }

  def toResult: String = {
    val iptcstr = iptc.mkString(" - ")
    val predstr = pred.mkString(" - ")
    f"$id - $hl - $url - IPTC >> $iptcstr >> PREDICTION >> $predstr"
  }

  def toMinimal: Article = copy(body = "", hl = "")

  lazy val documentVectorMlLib: Vector = {
    val normalize: INDArray = W2VLoader.normalize(W2VLoader.documentVector(this))
    //    val binary: INDArray = Transforms.round(normalize, false)
    MLLibUtil.toVector(normalize)
  }

  def addIptc(broad: Boolean): Article = {
    copy(iptc = if (broad) IPTC.toBroad(desc, 0) else IPTC.toIptc(desc))
  }
}

object Article {
  def safeString(body: String): String = body.replaceAll("[\n\\[\\]\\~]", " ").replaceAll("\\s+", " ")

  def serialize(a: Article) = {
    Array(
      a.id, a.hl, a.body, a.wc, a.date.getOrElse("N"), a.iptc.mkString("[", "~", "]"), a.url.getOrElse("N"), a.desc.mkString("[", "~", "]"), a.pred.mkString("[", "~", "]")).mkString("\t ") + "\t\t" + a.ann.values.map(Annotation.serialize).mkString("~")
  }

  def deserialize(string: String): Article = {
    val annSplit = string.split("\t\t")
    val s = annSplit(0).split("\t ")
    val a = if (annSplit.length > 1) annSplit(1) else ""
    if (s.length != 9) throw new IllegalStateException("Article parse error: \n" + string)
    Article(
      id = s(0),
      hl = s(1),
      body = s(2),
      wc = s(3).toInt,
      date = if (s(4) == "N") None else Some(s(4)),
      iptc = toSet(s, 5),
      url = if (s(6) == "N") None else Some(s(6)),
      desc = toSet(s, 7),
      pred = toSet(s, 8),
      ann = a match {
        case x if x.length > 0 => x.split("~").map(Annotation.deserialize).map(ann => (ann.id, ann)).toMap
        case _ => Map()
      })
  }

  def toSet(s: Array[String], i: Int): Set[String] = {
    s(i).substring(1, s(i).length - 1) match {
      case a: String if a.length > 0 => a.split("~").toSet
      case _ => Set()
    }
  }
}
