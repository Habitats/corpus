package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.W2VLoader
import no.habitats.corpus.models.Article
import no.habitats.corpus.npl.IPTC
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

import scala.collection.JavaConverters._

class FeedForwardIterator(allArticles: Array[Article], label: String, batchSize: Int) extends DataSetIterator {

  // 32 may be a good starting point,
  var counter                       = 0

  override def next(num: Int): DataSet = {
    val articles = allArticles.slice(cursor, cursor + num)

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(articles.size, W2VLoader.featureSize)
    val labels = Nd4j.create(articles.size, totalOutcomes)

    for (i <- articles.toList.indices) {
      // We want to preserve order
      val vectors = articles(i).ann.values.map(_.fb).map(W2VLoader.fromId).map(_.get)
      val combined = vectors.reduce((a, b) => a.add(b)).div(vectors.size)
      features.putRow(i, combined)

      // binary
      val v = if (articles(i).iptc.contains(label)) 1 else 0
      labels.putScalar(Array(i, v), 1.0)
    }

    counter += articles.size
    new DataSet(features, labels)
  }
  override def batch(): Int = Math.min(batchSize, Math.max(allArticles.size - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = allArticles.size
  override def inputColumns(): Int = W2VLoader.featureSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = util.Arrays.asList(label, "not_" + label)
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = counter < totalExamples()
}
