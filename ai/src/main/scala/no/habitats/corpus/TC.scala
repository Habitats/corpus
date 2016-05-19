package no.habitats.corpus

import java.io.File

import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.common.models.{Annotation, Article}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.io.Source
import scala.util.Try

/**
  * Static methods for misc formulas and measures
  */
@Deprecated
case class TC(rdd: RDD[Article]) {
  rdd.cache()
  val documentsWithTerm      = rdd.flatMap(_.ann.values).map(a => (a.id, 1)).reduceByKey(_ + _).collect.toMap
  val maxFrequencyInDocument = rdd.map(a => (a.id, Try(a.ann.map(_._2.mc).max).getOrElse(0))).collect.toMap
  val frequencySumInDocument = rdd.map(a => (a.id, Try(a.ann.map(_._2.mc).sum).getOrElse(0))).collect.toMap
  val documentCount          = rdd.count

  lazy val computed: RDD[Article] = {
    rdd.map(a => {
      val newAnnotations = a.ann.map(annotation => {
        val res = tfidf(annotation._2)
        (annotation._1, annotation._2.copy(tfIdf = res))
      })
      a.copy(ann = newAnnotations)
    })
    rdd.unpersist()
  }

  def tfidf(annotation: Annotation): Double = simple(annotation) * inverseFrequency(annotation)

  def simple(annotation: Annotation): Double = annotation.mc.toDouble / frequencySumInDocument(annotation.articleId)

  def log(annotation: Annotation): Double = 1 + Math.log(annotation.mc)

  def inverseFrequency(annotation: Annotation): Double = Math.log(documentCount.toDouble / documentsWithTerm(annotation.id))

  def inverseFrequencySmooth(annotation: Annotation): Double = Math.log(1 + (documentCount.toDouble / documentsWithTerm(annotation.id)))
}

case class TFIDF(documentsWithTerm: Map[String, Int], phrases: Set[String], documentCount: Int) {

  private val vocabSize  : Int           = phrases.size
  private val phraseIndex: Array[String] = phrases.toArray.sorted

  def contains(id: String) = phrases.contains(id)

  def toVector(article: Article): Vector = {
    val values: Seq[(Int, Double)] = for {
      i <- phraseIndex.indices
      w = phraseIndex(i) if article.ann.contains(w)
      annotation = article.ann(w)
    } yield (i, tfidf(article, annotation))
    Vectors.sparse(phraseIndex.size, values)
  }

  def inverseDocumentFrequency(annotation: Annotation): Double = documentsWithTerm.get(annotation.id).map(i => Math.log(documentCount.toDouble / i)).getOrElse(0)

  def termFrequency(article: Article, annotation: Annotation): Double = annotation.mc.toDouble / article.ann.map(_._2.mc).sum

  def tfidf(article: Article, annotation: Annotation): Double = termFrequency(article, annotation) * inverseDocumentFrequency(annotation)
}

object TFIDF {
  implicit val formats = Serialization.formats(NoTypeHints)

  def frequencyFilter(rdd: RDD[Article], phrases: Set[String]): RDD[Article] = rdd.map(_.filterAnnotation(ann => phrases.contains(ann.id))).filter(_.ann.nonEmpty)

  def apply(train: RDD[Article], threshold: Int): TFIDF = {
    val originalPhraseCount = train.flatMap(_.ann.keySet).distinct.count
    val documentsWithTerm: Map[String, Int] = train.flatMap(_.ann.values).map(a => (a.id, 1)).reduceByKey(_ + _).filter(_._2 > threshold).collect.toMap
    val phrases = documentsWithTerm.keySet
    val filteredTrain: RDD[Article] = frequencyFilter(train, phrases)
    val documentCount: Long = filteredTrain.count
    Log.v(s"Performaed TFIDF, and reduced vocab size from ${originalPhraseCount} to ${phrases.size}")
    new TFIDF(documentsWithTerm, phrases, documentCount.toInt)
  }

  def deserialize(name: String): TFIDF = {
    val s: String = Source.fromFile(new File(Config.modelPath + name).listFiles().filter(_.getName.contains("tfidf")).head).getLines().next()
    read[TFIDF](s)
  }

  def serialize(tfidf: TFIDF): String = write(tfidf)
}
