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
case class CorpusVectors(data: Array[(INDArray, Array[Int])]) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._2(labelIndex)
}

case class CorpusMatrix(data: Array[(Array[(String, Float)], Array[Int])]) extends CorpusDataset {
  override def label(articleIndex: Int, labelIndex: Int): Int = data(articleIndex)._2(labelIndex)
}

object CorpusDataset {

  def labelArray(article: Article): Array[Int] = IPTC.topCategories.map(i => if (article.iptc.contains(i)) 1 else 0).toArray

  def genW2VDataset(articles: RDD[Article]): CorpusVectors = {
    CorpusVectors(articles.collect().map(a => (W2VLoader.documentVector(a), labelArray(a))))
  }

  def genW2VMatrix(articles: RDD[Article]): CorpusMatrix = {
    CorpusMatrix(articles.map(a => {
      val annotationVectors = a.ann.values.toArray.sortBy(_.offset).map(an => (an.id, an.tfIdf.toFloat))
      (annotationVectors, labelArray(a))
    }).collect)
  }

  def genBoWDataset(articles: RDD[Article], tfidf: TFIDF): CorpusVectors = {
    CorpusVectors(articles.collect().map(a => (MLLibUtil.toVector(MlLibUtils.toVector(Some(tfidf), a)), IPTC.topCategories.map(i => if (a.iptc.contains(i)) 1 else 0).toArray)))
  }

  def testSpace(): CorpusVectors = {
    val articles = Fetcher.annotatedRddMinimal
    W2VLoader.preload(true, false)
    val articleVectors = CorpusDataset.genW2VDataset(articles)
    Log.v("hello")
    articleVectors
  }

  def testSpace2(): CorpusMatrix = {
    val articles = Fetcher.annotatedRddMinimal
    W2VLoader.preload(true, false)
    val articleVectors = CorpusDataset.genW2VMatrix(articles)
    Log.v("hello2")
    articleVectors
  }

  def testSpace3(): CorpusVectors = {
    val articles = Fetcher.annotatedRddMinimal
    W2VLoader.preload(true, false)
    val articleVectors = CorpusDataset.genBoWDataset(articles, TFIDF.deserialize("all-ffn-bow"))
    Log.v("hello3")
    articleVectors
  }
}