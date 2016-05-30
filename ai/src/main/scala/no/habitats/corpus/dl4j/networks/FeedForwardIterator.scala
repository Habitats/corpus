package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.{Config, W2VLoader}
import no.habitats.corpus.spark.CorpusVectors
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j

class FeedForwardIterator(training: CorpusVectors, label: Int, batchSize: Int) extends DataSetIterator {
  // 32 may be a good starting point,
  var counter = 0

  override def next(num: Int): DataSet = {
    val articles = training.data.slice(cursor, cursor + num)

    val features = Nd4j.create(articles.size, inputColumns)
    val labels = Nd4j.create(articles.size, totalOutcomes)

    for (i <- articles.indices) {
      val annotationIds: Set[(String, Float)] = articles(i)._2
      val articleId = articles(i)._1
      features.putRow(i, W2VLoader.documentVector(articleId, annotationIds))

      // binary
      labels.putScalar(Array(i, articles(i)._3(label)), 1.0)
    }

    counter += articles.size
    new DataSet(features, labels)
  }

  override def batch(): Int = Math.min(Config.miniBatchSize.getOrElse(batchSize), Math.max(training.data.size - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = training.data.size
  override def inputColumns(): Int = training.data.head._4
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = util.Arrays.asList("yes", "no")
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = counter < totalExamples()
}


