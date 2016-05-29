package no.habitats.corpus.spark

import no.habitats.corpus.common.{IPTC, TFIDF}
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.mllib.MlLibUtils
import org.apache.spark.rdd.RDD
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray

case class CorpusDataset(data: Array[(INDArray, Array[Int])], rdd: RDD[Article]) {
  lazy val articles = rdd.collect()
}

object CorpusDataset {
  def genW2VDataset(articles: RDD[Article]): CorpusDataset = {
    CorpusDataset(articles.map(a => (a.toDocumentVector, IPTC.topCategories.map(i => if (a.iptc.contains(i)) 1 else 0).toArray)).collect(), articles)
  }

  def genBoWDataset(articles: RDD[Article], tfidf: TFIDF): CorpusDataset = {
    CorpusDataset(articles.map(a => (MLLibUtil.toVector(MlLibUtils.toVector(Some(tfidf), a)), IPTC.topCategories.map(i => if (a.iptc.contains(i)) 1 else 0).toArray)).collect(), articles)
  }
}