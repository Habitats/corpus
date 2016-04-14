package no.habitats.corpus.dl4j.ex

import java.util

import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.factory.Nd4j

import scala.collection.JavaConverters._
import scala.util.Random

object Main extends App {
  println("This runs fine ...")
  val data: List[DataSet] = new MockIterator(1L).asScala.toList
  DataSet.merge(data.asJava)

  println("\nThis will crash ...")
  val data2: List[DataSet] = new MockIterator(2L).asScala.toList
  DataSet.merge(data2.asJava)
}

class MockIterator(seed: Long) extends DataSetIterator {
  val batchSize = 50
  var counter = 0
  val dataSetSize = 698
  val featureSize = 1000
  val labelSize = 3
  val r = new Random(seed)

  override def next(num: Int): DataSet = {
    val maxNumberOfFeatures = r.nextInt(100) + 1
    print(maxNumberOfFeatures + " ")

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(num, featureSize, maxNumberOfFeatures)
    val labels = Nd4j.create(num, labelSize, maxNumberOfFeatures)
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.zeros(num, maxNumberOfFeatures)
    val labelsMask = Nd4j.zeros(num, maxNumberOfFeatures)

    counter += num
    new DataSet(features, labels, featureMask, labelsMask)
  }
  override def batch(): Int = Math.min(batchSize, Math.abs(dataSetSize - counter))
  override def cursor(): Int = counter
  override def totalExamples(): Int = dataSetSize
  override def inputColumns(): Int = featureSize
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = throw new UnsupportedOperationException
  override def getLabels: util.List[String] = Seq("a", "b", "c").asJava
  override def totalOutcomes(): Int = getLabels.size
  override def reset(): Unit = counter = 0
  override def numExamples(): Int = totalExamples()
  override def next(): DataSet = next(batch)
  override def hasNext: Boolean = cursor() < totalExamples()
}