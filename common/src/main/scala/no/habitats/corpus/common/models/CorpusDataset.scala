package no.habitats.corpus.common.models

import no.habitats.corpus.common._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.Map
import scala.collection.immutable.ListMap

case class SimpleArticle(id: String, annotations: Map[String, Float], labels: Array[Int], featureSize: Int)

case class CorpusDataset(data: Array[SimpleArticle], private val transformer: (String, Map[String, Float]) => INDArray) {
  def toVector(articleId: String, annotationIds: Map[String, Float]): INDArray = {
    transformer(articleId, annotationIds)
  }
}

object CorpusDataset {

  def bowVector(articleId: String, annotationIds: Map[String, Float], phrases: Array[(String, Int)]): INDArray = {
    Nd4j.create(phrases.map(id => annotationIds.getOrElse(id._1, 0f)))
  }

  def documentVector(articleId: String, annotationIds: Map[String, Float]): INDArray = {
    val vectors: Iterable[INDArray] = annotationIds.map { case (id, tfidf) => wordVector(id).mul(tfidf) }
    val combined = vectors.reduce(_.addi(_))
    combined
  }

  def wordVector(annotationId: String): INDArray = {
    W2VLoader.fromId(annotationId).getOrElse(throw new IllegalStateException(s"Missing word vector for $annotationId!"))
  }

  def annotationSet(a: Article, tfidf: TFIDF, ordered: Boolean): Map[String, Float] = {
    def pair(an: Annotation): (String, Float) = (an.id, tfidf.tfidf(a, an).toFloat)
    val annotations: Iterable[Annotation] = a.ann.values.filter(an => tfidf.phrases.contains(an.id))
    if (ordered) ListMap(annotations.toList.sortBy(_.offset).map(pair): _ *) else annotations.map(pair).toMap
  }

  def labelArray(article: Article): Array[Int] = IPTC.topCategories.map(i => if (article.iptc.contains(i)) 1 else 0).toArray

  def genW2VDataset(articles: RDD[Article], tfidf: TFIDF): CorpusDataset = {
    CorpusDataset(articles.map(a => SimpleArticle(a.id, annotationSet(a, tfidf, ordered = false), labelArray(a), 1000)).filter(_.annotations.nonEmpty).collect(), (articleId, annotationIds) => documentVector(articleId, annotationIds))
  }

  def genBoWDataset(articles: RDD[Article], tfidf: TFIDF): CorpusDataset = {
    CorpusDataset(articles.map(a => SimpleArticle(a.id, annotationSet(a, tfidf, ordered = false), labelArray(a), tfidf.phrases.size)).filter(_.annotations.nonEmpty).collect(), (articles, annotationIds) => bowVector(articles, annotationIds, tfidf.phrasesList))
  }

  def genW2VMatrix(articles: RDD[Article], tfidf: TFIDF): CorpusDataset = {
    Log.v("Generating W2V matrix ...")
    val cutoff = 100 // avoid RNN going crazy
    val m = CorpusDataset(articles.map(a => {
      SimpleArticle(a.id, annotationSet(a, tfidf, ordered = true).take(cutoff), labelArray(a), 1000)
    }).filter(_.annotations.nonEmpty).collect, (annotationId, annotationIds) => wordVector(annotationId))
    Log.v("Generation complete!")
    m
  }

  // High level API for vector conversion
  def documentVector(article: Article, tfidf: TFIDF, ordered: Boolean): INDArray = {
    documentVector(article.id, annotationSet(article, tfidf, ordered))
  }

  def bowVector(article: Article, tfidf: TFIDF): INDArray = {
    bowVector(article.id, annotationSet(article, tfidf, false), tfidf.phrasesList)
  }

  def documentVectorMlLib(article: Article, tfidf: TFIDF, ordered: Boolean): Vector = {
    MLLibUtil.toVector(documentVector(article.id, annotationSet(article, tfidf, ordered)))
  }

  def bowVectorMlLib(article: Article, tfidf: TFIDF): Vector = {
    val set: Map[String, Float] = annotationSet(article, tfidf, ordered = false)
    val data = set.toList.map(i => (tfidf.phraseIndex(i._1), i._2.toDouble))
    Vectors.sparse(tfidf.phrases.size, data)
  }

  def toVector(tfidf: TFIDF, a: Article): Vector = {
    if (tfidf.name.contains("w2v")) CorpusDataset.documentVectorMlLib(a, tfidf, ordered = false)
    else CorpusDataset.bowVectorMlLib(a, tfidf)
  }

  def toINDArray(tfidf: TFIDF, a: Article): INDArray = {
    if (tfidf.name.contains("w2v")) CorpusDataset.documentVector(a, tfidf, ordered = false)
    else CorpusDataset.bowVector(a, tfidf)
  }
}