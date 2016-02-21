package no.habitats.corpus.dl4j

import java.io.File
import java.util

import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor

import scala.collection.mutable.ArrayBuffer

class W2VSentimentIterator(dataDirectory: String, wordVectors: WordVectors, batchSize: Int, truncateLength: Int, train: Boolean) extends DataSetIterator{
  val vectorSize = wordVectors.lookupTable.layerSize
  val p = new File(dataDirectory + "/aclImdb/" + (if(train) "train" else "test") + "/pos/")
  val n = new File(dataDirectory + "/aclImdb/" + (if(train) "train" else "test") + "/neg/")
  val positiveFiles = p.listFiles
  val negativeFiles = n.listFiles
//  val cursor = 0
  val tokenizerFactory = {
    val tf = new DefaultTokenizerFactory()
    tf.setTokenPreProcessor(new CommonPreprocessor)
    tf
  }

  private def nextDataSet(num: Int): DataSet = {
//    val reviews = ArrayBuffer(num)
//    for(i <- 0 until totalExamples) {
//      if(cursor % 2 == 0){
//        val posReviewNu
//      }
//    }
    null
  }

  override def next(num: Int): DataSet = {
    if(cursor >= positiveFiles.length + negativeFiles.length) throw new NoSuchElementException()
    return nextDataSet(num)
  }
  override def batch(): Int = ???
  override def cursor(): Int = ???
  override def totalExamples(): Int = ???
  override def inputColumns(): Int = ???
  override def setPreProcessor(preProcessor: DataSetPreProcessor): Unit = ???
  override def getLabels: util.List[String] = ???
  override def totalOutcomes(): Int = ???
  override def reset(): Unit = ???
  override def numExamples(): Int = ???
  override def next(): DataSet = ???
  override def hasNext: Boolean = ???
}
