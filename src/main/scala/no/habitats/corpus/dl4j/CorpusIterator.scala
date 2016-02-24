package no.habitats.corpus.dl4j

import java.util

import no.habitats.corpus.dl4j.CorpusIterator._
import no.habitats.corpus.models.Annotation
import no.habitats.corpus.npl.IPTC
import no.habitats.corpus.spark.RddFetcher
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

import scala.collection.JavaConverters._

class CorpusIterator(train: Boolean, batchSize: Int) extends DataSetIterator {
  var counter = 0

  override def next(num: Int): DataSet = {
    val articles = allArticles.slice(cursor, cursor + batch)

    val maxLength = Math.min(articles.map(_.ann.size).max, vectorSize)

    val features = Nd4j.create(articles.size, vectorSize, maxLength)
    val labels = Nd4j.create(articles.size, 2, maxLength)
    val featureMask = Nd4j.zeros(articles.size, maxLength)
    val labelsMask = Nd4j.zeros(articles.size, maxLength)

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
      val idx = if (articles(i).iptc.contains("sport")) 1 else 0
      val lastIdx = Math.min(tokens.size, maxLength)
      labels.putScalar(Array(i, idx, lastIdx - 1), 1.0)
      labelsMask.putScalar(Array(i, lastIdx - 1), 1.0)
    }

    counter += articles.size
    new DataSet(features, labels, featureMask, labelsMask)
  }
  override def batch(): Int = batchSize
  override def cursor(): Int = counter
  override def totalExamples(): Int = allArticles.size
  override def inputColumns(): Int = vectorSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = labels
  override def totalOutcomes(): Int = 2
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = cursor() < totalExamples()
}

object CorpusIterator {
  lazy val allArticles = RddFetcher.rdd.filter(_.iptc.nonEmpty).filter(_.ann.nonEmpty).collect
  lazy val vectors = FreebaseW2V.loadVectors()
  lazy val labels = IPTC.mediaTopicLabels.toList.asJava
  lazy val features = vectors.keySet
  lazy val vectorSize = vectors.values.head.length()
}
