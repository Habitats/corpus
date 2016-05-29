package no.habitats.corpus.common

import java.io.File

import no.habitats.corpus.common.models.{Annotation, Article}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

case class TFIDF(documentsWithTerm: Map[String, Int], phrases: Set[String], documentCount: Int) {

  private val vocabSize  : Int                         = phrases.size
  private val phraseIndex: Array[String]               = phrases.toArray.sorted
  private val memo       : mutable.LongMap[Vector] = mutable.LongMap[Vector]()

  def contains(id: String) = phrases.contains(id)

  def toVector(article: Article): Vector = {
    memo.getOrElseUpdate(article.id.toLong, {
      val values: Seq[(Int, Double)] = for {
        i <- phraseIndex.indices
        w = phraseIndex(i) if article.ann.contains(w)
        annotation = article.ann(w)
      } yield (i, tfidf(article, annotation))
      Vectors.sparse(phraseIndex.size, values)
    })
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
    val documentsWithTerm: Map[String, Int] = train.flatMap(_.ann.values).map(a => (a.id, 1)).reduceByKey(_ + _).filter(_._2 > Config.termFrequencyThreshold.getOrElse(threshold)).collect.toMap
    val phrases = documentsWithTerm.keySet
    val filteredTrain: RDD[Article] = frequencyFilter(train, phrases)
    val documentCount: Long = filteredTrain.count
    if (phrases.isEmpty) throw new IllegalStateException("TFIDF removed all phrases!")
    Log.v(s"Performaed TFIDF, and reduced vocab size from ${originalPhraseCount} to ${phrases.size}")
    new TFIDF(documentsWithTerm, phrases, documentCount.toInt)
  }

  def deserialize(name: String): TFIDF = {
    val s: String = Try(Source.fromFile(new File(Config.modelPath + name).listFiles().filter(_.getName.contains("tfidf")).head).getLines().next()) match {
      case Failure(ex) => throw new IllegalStateException(s"NO TFIDF CACHE! Failed fetching: ${Config.modelPath + name}")
      case Success(s) => s
    }
    read[TFIDF](s)
  }

  def serialize(tfidf: TFIDF): String = write(tfidf)
}
