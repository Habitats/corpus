package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{TFIDF, W2VLoader}
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j

class FeedForwardIterator(allArticles: Seq[Article], label: String, batchSize: Int, tfidf: Option[TFIDF] = None) extends DataSetIterator {
  if (tfidf.isEmpty) W2VLoader.preload(wordVectors = true, documentVectors = true)

  // 32 may be a good starting point,
  var counter = 0

  override def next(num: Int): DataSet = {
    val articles = allArticles.slice(cursor, cursor + num)

    val features = Nd4j.create(articles.size, inputColumns)
    val labels = Nd4j.create(articles.size, totalOutcomes)

    for (i <- articles.indices) {
      val article: Article = articles(i)
      val vector = tfidf match {
        case None => article.toDocumentVector
        case Some(v) => MLLibUtil.toVector(v.toVector(article))
      }
      features.putRow(i, vector)

      // binary
      val v = if (article.iptc.contains(label)) 1 else 0
      labels.putScalar(Array(i, v), 1.0)
    }

    counter += articles.size
    new DataSet(features, labels)
  }

  override def batch(): Int = Math.min(batchSize, Math.max(allArticles.size - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = allArticles.size
  override def inputColumns(): Int = tfidf match {
    case None => W2VLoader.featureSize
    case Some(v) => v.phrases.size
  }
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = util.Arrays.asList(label, "not_" + label)
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = counter < totalExamples()
}


