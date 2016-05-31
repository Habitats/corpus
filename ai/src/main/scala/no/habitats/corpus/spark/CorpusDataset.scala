package no.habitats.corpus.spark

import no.habitats.corpus.common._
import no.habitats.corpus.common.models.Article
import org.apache.spark.rdd.RDD
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.immutable.ListMap
import scala.collection.{Map, Set}

case class SimpleArticle(id: String, annotations: Map[String, Float], labels: Array[Int], featureSize: Int)

case class CorpusDataset(data: Array[SimpleArticle], private val transformer: (String, Map[String, Float]) => INDArray) {
  lazy val memo: scala.collection.mutable.LongMap[INDArray] = new scala.collection.mutable.LongMap[INDArray]

  def toVector(articleId: String, annotationIds: Map[String, Float]): INDArray = {
    if (Config.memo) synchronized(memo.getOrElseUpdate(articleId.toLong, transformer(articleId, annotationIds)))
    else transformer(articleId, annotationIds)
  }
}

object CorpusDataset {

  def bowVector(articleId: String, annotationIds: Map[String, Float], phrases: Set[String]): INDArray = {
    Nd4j.create(phrases.toArray.map(id => annotationIds.getOrElse(id, 0f)))
  }

  def documentVector(articleId: String, annotationIds: Map[String, Float]): INDArray = {
    val vectors: Iterable[INDArray] = annotationIds.map { case (id, tfidf) => wordVector(id, tfidf) }
    val combined = vectors.reduce(_.addi(_))
    combined
  }

  def wordVector(annotationId: String, tfidf: Float): INDArray = {
    W2VLoader.fromId(annotationId).map(_.mul(tfidf)).getOrElse(throw new IllegalStateException(s"Missing word vector for $annotationId!"))
  }

  def annotationSet(a: Article, tfidf: TFIDF, ordered: Boolean): Map[String, Float] = {
    if (ordered) ListMap(a.ann.values.toList.sortBy(_.offset).map(an => (an.id, tfidf.tfidf(a, an).toFloat)): _ *)
    else a.ann.values.toList.sortBy(_.offset).map(an => (an.id, tfidf.tfidf(a, an).toFloat)).toMap
  }

  def labelArray(article: Article): Array[Int] = IPTC.topCategories.map(i => if (article.iptc.contains(i)) 1 else 0).toArray

  def genW2VDataset(articles: RDD[Article], tfidf: TFIDF): CorpusDataset = {
    CorpusDataset(articles.map(a => SimpleArticle(a.id, annotationSet(a, tfidf, ordered = false), labelArray(a), 1000)).collect(), (articleId, annotationIds) => documentVector(articleId, annotationIds))
  }

  def genBoWDataset(articles: RDD[Article], tfidf: TFIDF): CorpusDataset = {
    CorpusDataset(articles.map(a => SimpleArticle(a.id, annotationSet(a, tfidf, ordered = false), labelArray(a), tfidf.phrases.size)).collect(), (articles, annotationIds) => bowVector(articles, annotationIds, tfidf.phrases))
  }

  def genW2VMatrix(articles: RDD[Article], tfidf: TFIDF): CorpusDataset = {
    CorpusDataset(articles.map(a => SimpleArticle(a.id, annotationSet(a, tfidf, ordered = true), labelArray(a), 1000)).collect, (annotationId, annotationIds) => wordVector(annotationId, annotationIds(annotationId)))
  }
}