package no.habitats.corpus.dl4j

import java.io.File

import no.habitats.corpus.models.Article
import no.habitats.corpus.spark.CorpusContext._
import no.habitats.corpus.spark.RddFetcher
import no.habitats.corpus.{Config, Log}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.AsyncDataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
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
  lazy val gVec: WordVectors = WordVectorSerializer.loadGoogleModel(gModel, true)

  lazy val split: Array[RDD[Article]] = RddFetcher.rdd.randomSplit(Array(0.8, 0.2), seed = 1L)
  lazy val train: RDD[Article] = split(0)
  lazy val test: RDD[Article] = split(1)

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
      .collect() // this takes a long time
      .toMap
  }

  def trainSparkRNN() = {
    val nEpochs = 5
    var net = multiLabelRNN()
    net.setUpdater(null)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val trainIter: List[DataSet] = new MultiLabelIterator(train).asScala.toList
    val testIter = new AsyncDataSetIterator(new MultiLabelIterator(test))
    val rddTrain: JavaRDD[DataSet] = sc.parallelize(trainIter)


    Log.v("Starting training ...")
    for (i <- 0 until nEpochs) {
      net = sparkNetwork.fitDataSet(rddTrain)
      Log.v(f"Epoch $i complete. Starting evaluation:")
      val eval = new Evaluation()
      testIter.asScala.foreach(t => {
        val features = t.getFeatureMatrix
        val labels = t.getLabels
        val inMask = t.getFeaturesMaskArray
        val outMask = t.getLabelsMaskArray
        val predicted = net.output(features, false, inMask, outMask)
        eval.evalTimeSeries(labels, predicted, outMask)
      })

      Log.v(eval.stats)
      testIter.reset()
    }
  }

  def minimal() = {
    val trainIter: List[DataSet] = new MultiLabelIterator(train).asScala.toList
    //    val trainIter: List[DataSet] = new MockIterator(1L).asScala.toList
    DataSet.merge(trainIter.asJava)
  }

  def trainRNN() = {
    val nEpochs = 5
    val net = multiLabelRNN()

    val trainIter = new AsyncDataSetIterator(new MultiLabelIterator(train))
    val testIter = new AsyncDataSetIterator(new MultiLabelIterator(test))
    Log.r("Starting training ...")
    for (i <- 0 until nEpochs) {
      net.fit(trainIter)
      trainIter.reset()
      val eval = new Evaluation()
      testIter.asScala.toList.foreach(t => {
        val features = t.getFeatureMatrix
        val labels = t.getLabels
        val inMask = t.getFeaturesMaskArray
        val outMask = t.getLabelsMaskArray
        val predicted = net.output(features, false, inMask, outMask)
        eval.evalTimeSeries(labels, predicted, outMask)
      })
      Log.r(prettyConfusion(eval))
      testIter.reset()
      val stats = Seq[(String, String)](
        "Epoch" -> f"$i%10d",
        "TP" -> f"${eval.truePositives.toInt}%5d",
        "FP" -> f"${eval.falsePositives.toInt}%5d",
        "FN" -> f"${eval.falseNegatives.toInt}%5d",
        "TN" -> f"${eval.trueNegatives.toInt}%5d",
        "Recall" -> f"${eval.recall}%.3f",
        "Precision" -> f"${eval.precision}%.3f",
        "Accuracy" -> f"${eval.accuracy}%.3f",
        "F-score" -> f"${eval.f1}%.3f"
      )
      if (i == 0) {
        Log.r(stats.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 2}s").format(s._1)).mkString(""))
      }
      Log.r(stats.map(s => (s"%${Math.max(s._1.length, s._2.toString.length) + 2}s").format(s._2)).mkString(""))
      Log.r(eval.stats)
    }
  }

  def prettyConfusion(eval: Evaluation[Nothing]): String = {
    eval.getConfusionMatrix.toCSV.split("\n")
      .map(_.split(",").zipWithIndex.map { case (k, v) => if (v == 0) f"$k%12s" else f"$k%6s" }.mkString(""))
      .mkString("\n", "\n", "")
  }

  def multiLabelRNN(): MultiLayerNetwork = {
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
      .layer(1, new RnnOutputLayer.Builder().activation("softmax").lossFunction(LossFunctions.LossFunction.MCXENT).nIn(200).nOut(17).build())
      .pretrain(false).backprop(true).build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    net.setListeners(new ScoreIterationListener(1))
    net
  }
}
