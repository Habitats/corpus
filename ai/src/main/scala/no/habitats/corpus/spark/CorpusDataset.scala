package no.habitats.corpus.spark

import no.habitats.corpus.common._
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.mllib.MlLibUtils
import org.apache.spark.rdd.RDD
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray

sealed trait CorpusDataset {
  def label(articleIndex: Int, labelIndex: Int): Int
}

case class CorpusVectors(data: Array[(String, Set[(String, Float)], Array[Int], Int)]) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._3(labelIndex)
}

case class CorpusMatrix(data: Array[(Array[(String, Float)], Array[Int])]) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._2(labelIndex)
}

object CorpusDataset {

  def annotationSet(a: Article): Set[(String, Float)] =  a.ann.map(an => (an._1, an._2.tfIdf.toFloat)).toSet

  def labelArray(article: Article): Array[Int] = IPTC.topCategories.map(i => if (article.iptc.contains(i)) 1 else 0).toArray

  def genW2VDataset(articles: RDD[Article]): CorpusVectors = {
    CorpusVectors(articles.collect().map(a => (a.id, annotationSet(a), labelArray(a), 1000)))
  }

  def genBoWDataset(articles: RDD[Article], size: Int): CorpusVectors = {
    CorpusVectors(articles.collect().map(a => (a.id, annotationSet(a), labelArray(a), size)))
  }

  def genW2VMatrix(articles: RDD[Article]): CorpusMatrix = {
    CorpusMatrix(articles.map(a => {
      val annotationVectors = a.ann.values.toArray.sortBy(_.offset).map(an => (an.id, an.tfIdf.toFloat))
      (annotationVectors, labelArray(a))
    }).collect)
  }
}