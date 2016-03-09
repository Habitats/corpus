package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.dl4j.FreebaseW2V
import no.habitats.corpus.dl4j.networks.CorpusIterator._
import no.habitats.corpus.models.{Annotation, Article}
import no.habitats.corpus.npl.IPTC
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

import scala.collection.JavaConverters._

class CorpusIterator(rdd: RDD[Article], label: Option[String]) extends DataSetIterator {

  // 32 may be a good starting point,
  val batchSize = 50
  var counter = 0
  lazy val allArticles = rdd
    .filter(_.iptc.nonEmpty).collect
    .map(a => a.copy(ann = a.ann.filter(an => vectors.contains(an._2.fb))))
    .filter(_.ann.nonEmpty)

  val categories: util.List[String] = label.fold(IPTC.topCategories.toList)(List(_)).asJava

  override def next(num: Int): DataSet = {
    val articles = allArticles.slice(cursor, cursor + num)
    val maxNumberOfFeatures = articles.map(_.ann.size).max

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(articles.size, featureSize, maxNumberOfFeatures)
    val labels = Nd4j.create(articles.size, totalOutcomes, maxNumberOfFeatures)
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.zeros(articles.size, maxNumberOfFeatures)
    val labelsMask = Nd4j.zeros(articles.size, maxNumberOfFeatures)

    for (i <- articles.toList.indices) {
      val tokens = articles(i).ann.values
        .filter(_.fb != Annotation.NONE)
        .map(_.fb)
        .filter(vectors.contains)
        .toList
      for (j <- tokens.indices) {
        val vector = vectors(tokens(j))
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
  override def batch(): Int = Math.min(batchSize, Math.max(allArticles.size - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = allArticles.size
  override def inputColumns(): Int = featureSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = categories
  override def totalOutcomes(): Int = if(label.isDefined) 2 else getLabels.size
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = cursor() < totalExamples()
}

object CorpusIterator {
  lazy val vectors = FreebaseW2V.loadVectors()
  lazy val features = vectors.keySet
  lazy val featureSize = vectors.values.head.length()
}
