package no.habitats.corpus.spark

import no.habitats.corpus.common._
import no.habitats.corpus.common.models.Article
import org.apache.spark.rdd.RDD
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.{Map, Set, mutable}

sealed trait CorpusDataset {
  def label(articleIndex: Int, labelIndex: Int): Int
}

case class CorpusVectors(data: Array[(String, Map[String, Float], Array[Int], Int)], transformer: (String, Map[String, Float]) => INDArray) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._3(labelIndex)
}

case class CorpusMatrix(data: Array[(Array[(String, Float)], Array[Int])], transformer: (String) => INDArray) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._2(labelIndex)
}

object CorpusDataset {
  lazy     val memo   : mutable.LongMap[INDArray] = new mutable.LongMap[INDArray]
  lazy     val bowmemo: mutable.LongMap[INDArray] = new mutable.LongMap[INDArray]

  def bowVector(articleId: String, annotationIds: Map[String, Float], phrases: Set[String]): INDArray = synchronized {
    bowmemo.getOrElseUpdate(articleId.toLong, Nd4j.create(phrases.toArray.map(id => annotationIds.getOrElse(id, 0f))))
  }

  def documentVector(articleId: String, annotationIds: Map[String, Float]): INDArray = synchronized {
    memo.getOrElseUpdate(articleId.toLong, {
      val vectors: Iterable[INDArray] = annotationIds.flatMap { case (id, tfidf) => W2VLoader.fromId(id).map(_.mul(tfidf)) }
      val combined = vectors.reduce(_.addi(_))
      combined
    })
  }

  def annotationSet(a: Article, tfidf: TFIDF): Map[String, Float] = a.ann.map(an => (an._1, tfidf.tfidf(a, an._2).toFloat))

  def labelArray(article: Article): Array[Int] = IPTC.topCategories.map(i => if (article.iptc.contains(i)) 1 else 0).toArray

  def genW2VDataset(articles: RDD[Article], tfidf: TFIDF): CorpusVectors = {
    CorpusVectors(articles.map(a => (a.id, annotationSet(a, tfidf), labelArray(a), 1000)).collect(), (articleId, annotationIds) => documentVector(articleId, annotationIds))
  }

  def genBoWDataset(articles: RDD[Article], tfidf: TFIDF): CorpusVectors = {
    CorpusVectors(articles.map(a => (a.id, annotationSet(a, tfidf), labelArray(a), tfidf.phrases.size)).collect(), (articles, annotationIds) => bowVector(articles, annotationIds, tfidf.phrases))
  }

  def genW2VMatrix(articles: RDD[Article], tfidf: TFIDF): CorpusMatrix = {
    CorpusMatrix(articles.map(a => {
      val annotationVectors = a.ann.values.toArray.sortBy(_.offset).map(an => (an.id, tfidf.tfidf(a, an).toFloat))
      (annotationVectors, labelArray(a))
    }).collect, (annotationId) => W2VLoader.fromId(annotationId).get)
  }
}