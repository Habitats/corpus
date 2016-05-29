package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.{Config, IPTC, W2VLoader}
import no.habitats.corpus.spark.CorpusMatrix
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

import scala.collection.JavaConverters._

class RNNIterator(allArticles: CorpusMatrix, label: String, batchSize: Int) extends DataSetIterator {
  W2VLoader.preload(wordVectors = true, documentVectors = false)

  // 32 may be a good starting point,
  var counter                       = 0
  val labelIndex = IPTC.topCategories.indexOf(label)

  override def next(num: Int): DataSet = {
    val articles = allArticles.data.slice(cursor, cursor + num)
    val maxNumberOfFeatures = articles.map(_._1.length).max

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(Array(articles.length, inputColumns, maxNumberOfFeatures), 'f')
    val labels = Nd4j.create(Array(articles.length, totalOutcomes, maxNumberOfFeatures), 'f')
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.create(Array(articles.length, maxNumberOfFeatures), 'f')
    val labelsMask = Nd4j.create(Array(articles.length, maxNumberOfFeatures), 'f')

    for (i <- articles.indices) {
      val annotations: Array[INDArray] = articles(i)._1
      for (j <- annotations.indices) {
        features.put(Array(NDArrayIndex.point(i), NDArrayIndex.all(), NDArrayIndex.point(j)), annotations(j))
        featureMask.putScalar(Array(i, j), 1.0)
      }
      // binary
      val labelPosition: Int = annotations.length - 1
      val label: Int = articles(i)._2(labelIndex)
      labels.putScalar(Array(i, label, labelPosition), 1.0)
      // Specify that an output exists at the final time step for this example
      labelsMask.putScalar(Array(i, labelPosition), 1.0)
    }

    counter += articles.length
    new DataSet(features, labels, featureMask, labelsMask)
  }
  override def batch(): Int = Math.min(Config.miniBatchSize.getOrElse(batchSize), Math.max(totalExamples - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = allArticles.data.length
  override def inputColumns(): Int = 1000
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = util.Arrays.asList("yes", "no")
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = counter < totalExamples()
}
