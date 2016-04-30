package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.W2VLoader
import no.habitats.corpus.models.Article
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.ops.transforms.Transforms

class FeedForwardIterator(allArticles: Array[Article], label: String, batchSize: Int, phrases: Array[String] = Array()) extends DataSetIterator {

  // 32 may be a good starting point,
  var counter = 0

  override def next(num: Int): DataSet = {
    val articles = allArticles.slice(cursor, cursor + num)

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(articles.size,inputColumns )
    val labels = Nd4j.create(articles.size, totalOutcomes)

    for (i <- articles.toList.indices) {
      // We want to preserve order
      features.putRow(i, if(phrases.isEmpty) articles(i).toND4JDocumentVector else MLLibUtil.toVector(articles(i).toVector(phrases)))

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
  override def inputColumns(): Int = if(phrases.isEmpty) W2VLoader.featureSize else phrases.size
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = util.Arrays.asList(label, "not_" + label)
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = counter < totalExamples()
}
