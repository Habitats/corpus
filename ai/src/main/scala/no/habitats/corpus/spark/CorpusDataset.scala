package no.habitats.corpus.spark

import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{IPTC, TFIDF, W2VLoader}
import no.habitats.corpus.mllib.MlLibUtils
import org.apache.spark.rdd.RDD
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray

sealed trait CorpusDataset {
  def label(articleIndex: Int, labelIndex: Int): Int
}
case class CorpusVectors(data: Array[(INDArray, Array[Int])]) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._2(labelIndex)
}

case class CorpusMatrix(data: Array[(Array[INDArray], Array[Int])]) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._2(labelIndex)
}

object CorpusDataset {

  def labelArray(article: Article): Array[Int] = IPTC.topCategories.map(i => if (article.iptc.contains(i)) 1 else 0).toArray

  def genW2VDataset(articles: RDD[Article]): CorpusVectors = {
    CorpusVectors(articles.map(a => (a.toDocumentVector, labelArray(a))).collect)
  }

  def genW2VMatrix(articles: RDD[Article]): CorpusMatrix = {
    CorpusMatrix(articles.map(a => {
      val annotationVectors: Array[INDArray] = a.ann.values.toArray.sortBy(_.offset).map(_.id).map(W2VLoader.fromId).filter(_.isDefined).map(_.get)
      (annotationVectors, labelArray(a))
    }).collect)
  }

  def genBoWDataset(articles: RDD[Article], tfidf: TFIDF): CorpusVectors = {
    CorpusVectors(articles.map(a => (MLLibUtil.toVector(MlLibUtils.toVector(Some(tfidf), a)), IPTC.topCategories.map(i => if (a.iptc.contains(i)) 1 else 0).toArray)).collect())
  }
}