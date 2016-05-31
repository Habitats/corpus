package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.models.CorpusDataset
import no.habitats.corpus.common.{Config, Log}
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j

import scala.collection.Map
import scala.util.{Failure, Success, Try}

class FeedForwardIterator(training: CorpusDataset, labelIndex: Int, batchSize: Int) extends DataSetIterator {
  // 32 may be a good starting point,
  var counter = 0

  override def next(num: Int): DataSet = {
    val articles = training.data.slice(cursor, cursor + num)

    val features = Nd4j.create(articles.size, inputColumns)
    val labels = Nd4j.create(articles.size, totalOutcomes)

    for (i <- articles.indices) {
      val annotationIds: Map[String, Float] = articles(i).annotations
      val articleId = articles(i).id
      val vector: INDArray = training.toVector(articleId, annotationIds)
      features.putRow(i, vector)

      // binary
      labels.putScalar(Array(i, articles(i).labels(labelIndex)), 1.0)
    }

    counter += articles.size
    new DataSet(features, labels)
  }

  override def batch(): Int = Math.min(Config.miniBatchSize.getOrElse(batchSize), Math.max(training.data.size - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = training.data.size
  override def inputColumns(): Int = training.data.head.featureSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = util.Arrays.asList("yes", "no")
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = counter < totalExamples()
}


