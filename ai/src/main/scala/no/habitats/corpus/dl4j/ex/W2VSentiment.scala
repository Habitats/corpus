package no.habitats.corpus.dl4j.ex

import java.io.File
import java.net.URL

import no.habitats.corpus.common.Log
import org.apache.commons.io.FilenameUtils
import org.deeplearning4j.datasets.iterator.AsyncDataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.examples.word2vec.sentiment.SentimentExampleIterator
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions

import scala.collection.JavaConverters._
import scala.sys.process._

object W2VSentiment {

  /** Data URL for downloading */
  val DATA_URL          = "http://ai.stanford.edu/~amaas/data/sentiment/aclImdb_v1.tar.gz"
  /** Location to save and extract the training/testing data */
  val DATA_PATH         = FilenameUtils.concat(System.getProperty("java.io.tmpdir"), "dl4j_w2vSentiment/")
  /** Location (local file system) for the Google News vectors. Set this manually. */
  val WORD_VECTORS_PATH = "D:/Archive/corpus/w2v/GoogleNews-vectors-negative300.bin"

  def downloadData() = {
    val dir = new File(DATA_PATH)
    if (dir.exists()) dir.mkdir()

    val archivePath = DATA_PATH + "aclImdb_v1.tar.gz"
    val archiveFile = new File(archivePath)
    if (!archiveFile.exists) {
      Log.v("Downloading ... " + archivePath)
      new URL(archivePath) #> archiveFile !!
    } else {
      Log.v(archivePath + " already exisits")
    }
  }

  def main(args: Array[String]) = {
    downloadData()

    val batchSize = 50
    val vectorSize = 300
    val nEpochs = 5
    val truncateReviewsToLength = 300 // truncate reviews greater than 300

    val conf = new NeuralNetConfiguration.Builder()
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
      .updater(Updater.RMSPROP)
      .regularization(true).l2(1e-5)
      .weightInit(WeightInit.XAVIER)
      .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue).gradientNormalizationThreshold(1.0)
      .learningRate(0.0018)
      .list(2)
      .layer(0, new GravesLSTM.Builder()
        .nIn(vectorSize)
        .nOut(200)
        .activation("softsign")
        .build()
      )
      .layer(1, new RnnOutputLayer.Builder()
        .activation("softmax")
        .lossFunction(LossFunctions.LossFunction.MCXENT)
        .nIn(200)
        .nOut(2)
        .build()
      )
      .pretrain(false).backprop(true).build()

    val net = new MultiLayerNetwork(conf)
    net.init()
    net.setListeners(new ScoreIterationListener(1))

    val wordVectors = WordVectorSerializer.loadGoogleModel(new File(WORD_VECTORS_PATH), true, false)
    val train = new AsyncDataSetIterator(new SentimentExampleIterator(DATA_PATH, wordVectors, batchSize, truncateReviewsToLength, true))
    val test = new AsyncDataSetIterator(new SentimentExampleIterator(DATA_PATH, wordVectors, batchSize, truncateReviewsToLength, false))

    Log.v("Starting training ...")
    for (i <- 0 until nEpochs) {
      net.fit(train)
      train.reset()
      Log.v(f"Epoch $i complete. Starting evaluation:")

      val eval = new Evaluation()
      test.asScala.toList.foreach(t => {
        val features = t.getFeatureMatrix
        val labels = t.getLabels
        val inMask = t.getFeaturesMaskArray
        val outMask = t.getLabelsMaskArray
        val predicted = net.output(features, false, inMask, outMask)
        eval.evalTimeSeries(labels, predicted, outMask)
      })
      test.reset()
      Log.v(eval.stats)
    }
  }
}
