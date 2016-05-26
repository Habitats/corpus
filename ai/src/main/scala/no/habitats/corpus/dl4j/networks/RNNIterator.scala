package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, IPTC, W2VLoader}
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

import scala.collection.JavaConverters._

class RNNIterator(allArticles: Array[Article], label: Option[String], batchSize: Int) extends DataSetIterator {
  W2VLoader.preload(wordVectors = true, documentVectors = false)

  // 32 may be a good starting point,
  var counter                       = 0
  val categories: util.List[String] = label.fold(IPTC.topCategories.toList)(List(_)).asJava

  override def next(num: Int): DataSet = {
    val articles = allArticles.slice(cursor, cursor + num)
    val maxNumberOfFeatures = articles.map(_.ann.size).max

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(Array(articles.size, W2VLoader.featureSize, maxNumberOfFeatures), 'f')
    val labels = Nd4j.create(Array(articles.size, totalOutcomes, maxNumberOfFeatures), 'f')
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.create(Array(articles.size, maxNumberOfFeatures), 'f')
    val labelsMask = Nd4j.create(Array(articles.size, maxNumberOfFeatures), 'f')

    for (i <- articles.toList.indices) {
      val tokens = articles(i).ann.values
        // We want to preserve order
        .toSeq.sortBy(ann => ann.offset)
        .map(ann => (ann.tfIdf, ann.fb))
        .toList
      for (j <- tokens.indices) {
        val vector: INDArray = W2VLoader.fromId(tokens(j)._2).get.mul(tokens(j)._1)
        features.put(Array(NDArrayIndex.point(i), NDArrayIndex.all(), NDArrayIndex.point(j)), vector)
        featureMask.putScalar(Array(i, j), 1.0)
      }
      // binary
      if (label.isDefined) {
        val v = if (articles(i).iptc.contains(label.get)) 1 else 0
        labels.putScalar(Array(i, v, tokens.size - 1), 1.0)
      }
      // multilabel
      else {
        getLabels.asScala.map(articles(i).iptc.contains).zipWithIndex.filter(_._1).map {
          case (k, v) => labels.putScalar(Array(i, v, tokens.size - 1), 1.0)
        }
      }
      // Specify that an output exists at the final time step for this example
      labelsMask.putScalar(Array(i, tokens.size - 1), 1.0)
    }

    counter += articles.size
    new DataSet(features, labels, featureMask, labelsMask)
  }
  override def batch(): Int = Math.min(Config.miniBatchSize.getOrElse(batchSize), Math.max(allArticles.size - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = allArticles.size
  override def inputColumns(): Int = W2VLoader.featureSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = categories
  override def totalOutcomes(): Int = if (label.isDefined) 2 else getLabels.size
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = counter < totalExamples()
}
