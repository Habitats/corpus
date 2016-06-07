package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.models.CorpusDataset
import no.habitats.corpus.common.{Config, IPTC, Log}
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

class RNNIterator(articles: CorpusDataset, label: String, batchSize: Int) extends DataSetIterator {
  var counter    = 0
  val labelIndex = IPTC.topCategories.indexOf(label)

  override def next(num: Int): DataSet = {
    val articleBatch = articles.data.slice(cursor, cursor + num)
    val maxNumberOfFeatures = articleBatch.map(_.annotations.size).max

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(Array(articleBatch.length, inputColumns, maxNumberOfFeatures), 'f')
    val labels = Nd4j.create(Array(articleBatch.length, totalOutcomes, maxNumberOfFeatures), 'f')
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.create(Array(articleBatch.length, maxNumberOfFeatures), 'f')
    val labelsMask = Nd4j.create(Array(articleBatch.length, maxNumberOfFeatures), 'f')

    for (i <- articleBatch.indices) {
      val annotations: Array[INDArray] = articleBatch(i).annotations.map { case (id, tfidf) => articles.toVector(id, articleBatch(i).annotations) }.toArray
      for (j <- annotations.indices) {
        features.put(Array(NDArrayIndex.point(i), NDArrayIndex.all(), NDArrayIndex.point(j)), annotations(j))
        featureMask.putScalar(Array(i, j), 1.0)
      }
      // binary
      val labelPosition: Int = annotations.length - 1
      val label: Int = articleBatch(i).labels(labelIndex)
      labels.putScalar(Array(i, label, labelPosition), 1.0)
      // Specify that an output exists at the final time step for this example
      labelsMask.putScalar(Array(i, labelPosition), 1.0)
    }

    counter += articleBatch.length
    new DataSet(features, labels, featureMask, labelsMask)
  }
  override def batch(): Int = Math.min(Config.miniBatchSize.getOrElse(batchSize), Math.max(totalExamples - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = articles.data.length
  override def inputColumns(): Int = articles.data.head.featureSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = util.Arrays.asList("yes", "no")
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch())
  override def hasNext: Boolean = counter < totalExamples()
}
