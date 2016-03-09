package no.habitats.corpus.dl4j.networks

import java.util

import no.habitats.corpus.dl4j.FreebaseW2V
import no.habitats.corpus.dl4j.networks.FeedForwardIterator._
import no.habitats.corpus.models.Article
import no.habitats.corpus.npl.IPTC
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j

import scala.collection.JavaConverters._

/**
  * Created by mail on 03.03.2016.
  */
class FeedForwardIterator(rdd: RDD[Article], label: Option[String] = None) extends DataSetIterator {

  val batchSize = 50
  var counter = 0
  lazy val allArticles = rdd
    .filter(_.iptc.nonEmpty).collect
    .map(a => a.copy(ann = a.ann.filter(an => vectors.contains(an._2.fb))))
    .filter(_.ann.nonEmpty)

  val labels: util.List[String] = {
    val l = label.map(Seq(_)).getOrElse(IPTC.topCategories.toList)
    l.asJava
  }

  override def next(num: Int): DataSet = {
    val articles = allArticles.slice(cursor, cursor + num)

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(articles.size, featureSize)
    val labels = Nd4j.create(articles.size, getLabels.size)
    for (article <- articles) {

    }

    counter += articles.size
    new DataSet(features, labels)
  }
  override def batch(): Int = Math.min(batchSize, Math.max(allArticles.size - counter, 0))
  override def cursor(): Int = counter
  override def totalExamples(): Int = allArticles.size
  override def inputColumns(): Int = featureSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = labels
  override def totalOutcomes(): Int = getLabels.size
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = cursor() < totalExamples()
}

object FeedForwardIterator {
  lazy val vectors = FreebaseW2V.loadVectors()
  lazy val features = vectors.keySet
  lazy val featureSize = vectors.size
}