package no.habitats.corpus.common

import java.io.File

import no.habitats.corpus.common.models.{Annotation, Article}
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.immutable.SortedSet
import scala.io.Source
import scala.util.{Failure, Success, Try}

case class TFIDF(documentsWithTerm: Map[String, Int], phrasess: Set[String], documentCount: Int, name: String) {

  lazy val phrasesList: Array[(String, Int)] = phrases.zipWithIndex.toArray
  lazy val phraseIndex: Map[String, Int]     = phrases.zipWithIndex.toMap
  // Hack to get phrases(s) to deserialize into SortedSet correctly ...
  lazy val phrases    : SortedSet[String]    = documentsWithTerm.keySet.to[SortedSet]

  def contains(id: String) = phrases.contains(id)

  def inverseDocumentFrequency(annotation: Annotation): Double = documentsWithTerm.get(annotation.id).map(i => Math.log(documentCount.toDouble / i)).getOrElse(0)

  def termFrequency(article: Article, annotation: Annotation): Double = annotation.mc.toDouble / article.ann.map(_._2.mc).sum

  def tfidf(article: Article, annotation: Annotation): Double = termFrequency(article, annotation) * inverseDocumentFrequency(annotation)
}

object TFIDF {
  implicit val formats = Serialization.formats(NoTypeHints)

  def frequencyFilter(rdd: RDD[Article], phrases: Set[String]): RDD[Article] = rdd.map(_.filterAnnotation(ann => phrases.contains(ann.id))).filter(_.ann.nonEmpty)

  def apply(train: RDD[Article], threshold: Int, path: String): TFIDF = {

    def docsWithTerm(train: RDD[Article], threshold: Int): Map[String, Int] = {
      val documentsWithTerm: Map[String, Int] = train.flatMap(_.ann.values).map(a => (a.id, 1)).reduceByKey(_ + _).filter(_._2 > Config.termFrequencyThreshold.getOrElse(threshold)).collect.toMap
      // Optional low-pass filter
      //      val totalDocumentCount = train.count
      //      val filteredDocumentsWithTerm = documentsWithTerm.filter(_._2 < totalDocumentCount * 0.8)
      //      filteredDocumentsWithTerm
      documentsWithTerm
    }

    val originalPhraseCount = train.flatMap(_.ann.keySet).distinct.count
    val documentsWithTerm = docsWithTerm(train, threshold)
    val phrases = documentsWithTerm.keySet.to[SortedSet]

    // This didn't have much effect
    // .intersect(Config.dataFile(Config.dataPath + "nyt/time/excluded_ids_time.txt").getLines().map(_.trim).toSet)

    val filteredTrain: RDD[Article] = frequencyFilter(train, phrases)
    val documentCount: Long = filteredTrain.count
    if (phrases.isEmpty) throw new IllegalStateException("TFIDF removed all phrases!")
    Log.v(s"Performaed TFIDF, and reduced vocab size from ${originalPhraseCount} to ${phrases.size}")
    val tfidf = new TFIDF(documentsWithTerm, phrases, documentCount.toInt, path)
    Log.saveToFile(TFIDF.serialize(tfidf), path + "tfidf.txt", overwrite = true)
    tfidf
  }

  def deserialize(path: String): TFIDF = {
    val s: String = Try(Source.fromFile(new File(path).listFiles().filter(_.getName.contains("tfidf")).head).getLines().next()) match {
      case Failure(ex) => throw new IllegalStateException(s"NO TFIDF CACHE! Failed fetching: ${path}")
      case Success(s) => s
    }
    read[TFIDF](s)
  }

  private def serialize(tfidf: TFIDF): String = write(tfidf)
}
