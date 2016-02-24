package no.habitats.corpus.dl4j

import java.io.File

import no.habitats.corpus.spark.CorpusContext._
import no.habitats.corpus.{Config, Log}
import org.apache.spark.api.java.JavaRDD
import org.deeplearning4j.datasets.iterator.AsyncDataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions

import scala.collection.JavaConverters._

object FreebaseW2V {

  lazy val gModel = new File(Config.dataPath + "w2v/freebase-vectors-skipgram1000.bin")
  lazy val gVec = WordVectorSerializer.loadGoogleModel(gModel, true)

  def cacheWordVectors() = {
    sc.textFile(Config.dataPath + "nyt/combined_ids_0.5.txt")
      .map(_.substring(22, 35).trim)
      .filter(gVec.hasWord).map(fb => (fb, gVec.getWordVector(fb)))
      .map(a => f"${a._1}, ${a._2.toSeq.mkString(", ")}")
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "fb_w2v_0.5")
  }

  def loadVectors(filter: Set[String] = Set.empty): Map[String, INDArray] = {
    sc.textFile(Config.dataPath + "nyt/fb_w2v_0.5.txt")
      .map(_.split(", "))
      .filter(arr => filter.isEmpty || filter.contains(arr(0)))
      .map(arr => (arr(0), arr.toSeq.slice(1, arr.length).map(_.toFloat).toArray))
      .map(arr => (arr._1, Nd4j.create(arr._2)))
      .collect()
      .toMap
  }

  def trainSparkRNN() = {
    val batchSize = 50
    val nEpochs = 5
    var net = rnnNetwork()
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val train: List[DataSet] = new CorpusIterator(true, batchSize).asScala.toList
    val test: List[DataSet] = new CorpusIterator(false, batchSize).asScala.toList
    val rddTrain: JavaRDD[DataSet] = sc.parallelize(train)

    Log.v("Starting training ...")
    for (i <- 0 until nEpochs) {
      net = sparkNetwork.fitDataSet(rddTrain)
      Log.v(f"Epoch $i complete. Starting evaluation:")
      val eval = new Evaluation()
      for (ds <- test) {
        ds.asScala.toList.foreach(t => {
          val features = t.getFeatureMatrix
          val labels = t.getLabels
          val inMask = t.getFeaturesMaskArray
          val outMask = t.getLabelsMaskArray
          val predicted = net.output(features, false, inMask, outMask)
          eval.evalTimeSeries(labels, predicted, outMask)
        })
      }
      Log.v(eval.stats)
    }
  }

  def trainRNN() = {
    val batchSize = 50
    val nEpochs = 5
    val net = rnnNetwork()

    val train = new AsyncDataSetIterator(new CorpusIterator(true, batchSize))
    val test = new AsyncDataSetIterator(new CorpusIterator(false, batchSize))

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

  def rnnNetwork(): MultiLayerNetwork = {
    val vectorSize = 1000
    val conf = new NeuralNetConfiguration.Builder()
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
      .updater(Updater.RMSPROP)
      .regularization(true).l2(1e-5)
      .weightInit(WeightInit.XAVIER)
      .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue).gradientNormalizationThreshold(1.0)
      .learningRate(0.0018)
      .list(2)
      .layer(0, new GravesLSTM.Builder().nIn(vectorSize).nOut(200).activation("softsign").build())
      .layer(1, new RnnOutputLayer.Builder().activation("softmax").lossFunction(LossFunctions.LossFunction.MCXENT).nIn(200).nOut(2).build())
      .pretrain(false).backprop(true).build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    net.setUpdater(null)
    net.setListeners(new ScoreIterationListener(1))
    net
  }
}
