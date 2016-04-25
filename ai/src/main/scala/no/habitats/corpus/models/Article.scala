package no.habitats.corpus.models

import java.util.concurrent.atomic.AtomicInteger

import com.nytlabs.corpus.NYTCorpusDocument
import no.habitats.corpus.npl.IPTC
import no.habitats.corpus.npl.extractors.OpenNLP
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

case class Article(id: String,
                   hl: String = null,
                   body: String = null,
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

  def filterAnnotation(f: Annotation => Boolean): Article = copy(ann = ann.filter{case (id, an) => f(an)})

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

  def toVector(phrases: Seq[String]): Vector = {
    toVectorSparse(phrases)
  }

  def toVectorDense(phrases: Seq[String]): Vector = {
    val row = phrases.map(w => if (ann.contains(w)) ann(w).tfIdf else 0).toArray
    Vectors.dense(row)
  }

  def toVectorSparse(phrases: Seq[String]): Vector = {
    val values: Seq[(Int, Double)] = for {
      i <- phrases.indices
      w = phrases(i) if ann.contains(w) && ann(w).tfIdf > 0
    } yield (i, ann(w).tfIdf)
    Vectors.sparse(phrases.size, values)
  }

  lazy val names = OpenNLP.nameFinderCounts(body)

  def addNames(): Article = {
    val index = new AtomicInteger(ann.size)
    val updatedAnn = names.map { case (name, (count, kind)) => Annotation.fromName(id, index.incrementAndGet(), name, count, kind) }
    copy(ann = updatedAnn.map(t => t.phrase -> t).toMap)
  }

  def addIptc(broad: Boolean): Article = {
    copy(iptc = if (broad) IPTC.toBroad(desc, 0) else IPTC.toIptc(desc))
  }
}

object Article {

  import scala.collection.JavaConverters._

  implicit def stringToOption(s: String): Option[String] = Option(s)
  implicit def intToOption(s: Integer): Option[String] = if (s != null) Some(s.toString) else None
  implicit def getEither(s: (String, String)): String = if (s._1 != null) s._1 else s._2

  def allDescriptors(a: NYTCorpusDocument): Set[String] = {
    val tax = a.getTaxonomicClassifiers.asScala.map(_.split("/").last).toSet
    val desc = (a.getDescriptors.asScala ++ a.getOnlineDescriptors.asScala ++ a.getGeneralOnlineDescriptors.asScala).toSet
    desc.union(tax).map(_.toLowerCase)
  }

  def apply(a: NYTCorpusDocument): Article = {
    new Article(
      id = a.getGuid.toString,
      hl = (a.getHeadline, a.getOnlineHeadline),
      body = a.getBody,
      wc = if (a.getWordCount != null) a.getWordCount else if (a.getBody != null) a.getBody.split("\\s+").length else 0,
      desc = allDescriptors(a),
      date = a.getPublicationDate.getTime.toString,
      url = a.getUrl.toString
    )
  }
}
