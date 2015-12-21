package no.habitats.corpus.models

import com.nytlabs.corpus.NYTCorpusDocument
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.JavaConverters._

case class Article(id: String,
                   hl: String = "",
                   body: String = "NONE",
                   wc: Int = -1,
                   date: Option[String] = None,
                   iptc: Set[String] = Set(),
                   url: Option[String] = None,
                   desc: Set[String] = Set(),
                   pred: Set[String] = Set(),
                   ann: Map[String, Annotation] = Map()) {

  override def toString: String = {
    val iptcstr = iptc.mkString(", ")
    val annstr = ann.values.map(_.phrase).mkString(", ")
    f"$id - $hl >> TAGS >> $iptcstr >> PHRASES >> $annstr"
  }

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
//    f"$id - TAGS >> $iptcstr%150s >> PREDICTION >> $predstr"
    f"$id - $hl - $url - IPTC >> $iptcstr >> PREDICTION >> $predstr"
  }

  def toVector(phrases: Seq[String]): Vector = {
    toVectorSparse(phrases)
  }

  def toVectorDense(phrases: Seq[String]): Vector = {
    val row = phrases.map(w => if (ann.contains(w)) ann(w).tfIdf else 0).toArray
    Vectors.dense(row)
  }

  def toVectorSparse(phrases: Seq[String]): Vector = {
    val values: Seq[(Int, Double)] = for {
      i <- 0 until phrases.size
      w = phrases(i) if ann.contains(w) && ann(w).tfIdf > 0
    } yield (i, ann(w).tfIdf)
    Vectors.sparse(phrases.size, values)
  }

  def equals(o: Article): Boolean = {
    id == o.id &&
      hl == o.hl &&
      body == o.body &&
      wc == o.wc &&
      desc == o.desc &&
      pred == o.pred &&
      date == o.date &&
      iptc == o.iptc &&
      url == o.url &&
      ann == o.ann
  }

  def strip = Article(id = id, hl = hl, iptc = iptc, pred = pred, ann = ann, wc = wc, desc = desc)
}

case class ArticleWrapper(a: NYTCorpusDocument) {
  implicit def stringToOption(s: String): Option[String] = if (s != null) Some(s) else None

  implicit def intToOption(s: Integer): Option[String] = if (s != null) Some(s.toString) else None

  implicit def getEither(s: (String, String)): String = if (s._1 != null) s._1 else s._2

  def allDescriptors(a: NYTCorpusDocument): Set[String] = {
    val tax = a.getTaxonomicClassifiers.asScala.map(_.split("/").last).toSet
    val desc = (a.getDescriptors.asScala ++ a.getOnlineDescriptors.asScala ++ a.getGeneralOnlineDescriptors.asScala).toSet
    desc.union(tax).map(_.toLowerCase)
  }

  def toArticle: Article = Article(
    id = a.getGuid.toString,
    hl = (a.getHeadline, a.getOnlineHeadline),
    body = if (a.getBody == null) "NONE" else a.getBody,
    wc = if (a.getWordCount != null) a.getWordCount else a.getBody.split("\\s+").length,
    desc = allDescriptors(a),
    date = a.getPublicationDate.getTime.toString,
    url = a.getUrl.toString
  )
}

