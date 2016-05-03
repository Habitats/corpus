package no.habitats.corpus.dl4j

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.dl4j.networks._
import no.habitats.corpus.spark.Cacher
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.{AsyncDataSetIterator, DataSetIterator}
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.joda.time.DateTime
import org.nd4j.linalg.dataset.DataSet

import scala.collection.JavaConverters._

object FreebaseW2V extends RddSerializer{

  lazy val gModel            = new File(Config.dataPath + "w2v/freebase-vectors-skipgram1000.bin")
  lazy val gVec: WordVectors = WordVectorSerializer.loadGoogleModel(gModel, true)

  def cacheWordVectors() = {
    val rdd = sc.textFile(Config.combinedIds)
      .map(_.substring(22, 35).trim)
      .filter(gVec.hasWord).map(fb => (fb, gVec.getWordVector(fb)))
      .map(a => f"${a._1}, ${a._2.toSeq.mkString(", ")}")

    saveAsText(rdd, "fb_to_w2v")
  }

  def cacheWordVectorIds() = {
    sc.parallelize(W2VLoader.vectors.keys.toSeq)
      .coalesce(1, shuffle = true).saveAsTextFile(Config.cachePath + "w2v_ids" + DateTime.now.secondOfDay.get)
  }

  def trainSparkRNN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    var net = RNN.createBinary(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val trainIter: List[DataSet] = new RNNIterator(neuralPrefs.train.collect(), Some(label), batchSize = neuralPrefs.minibatchSize).asScala.toList
    val testIter = new AsyncDataSetIterator(new RNNIterator(neuralPrefs.validation.collect(), Some(label), batchSize = neuralPrefs.minibatchSize))
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, 200, 2)
      val eval = NeuralEvaluation(net, testIter, i, label)
      eval.log()
      testIter.reset()
    }

    net
  }

  def trainSparkFFN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    var net = FeedForward.create(neuralPrefs)
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val testIter: DataSetIterator = new FeedForwardIterator(neuralPrefs.validation.collect(), label, batchSize = neuralPrefs.minibatchSize)
    val trainIter: List[DataSet] = new FeedForwardIterator(neuralPrefs.train.collect(), label, batchSize = neuralPrefs.minibatchSize).asScala.toList
    val rddTrain: RDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net = sparkNetwork.fitDataSet(rddTrain, 200, 2)
      val eval = NeuralEvaluation(net, testIter, i, label)
      eval.log()
      testIter.reset()
    }

    net
  }

  def trainBinaryRNN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = RNN.createBinary(neuralPrefs)
    val trainIter = new RNNIterator(neuralPrefs.train.collect(), Some(label), batchSize = neuralPrefs.minibatchSize)
    val testIter = new RNNIterator(neuralPrefs.validation.collect(), Some(label), batchSize = neuralPrefs.minibatchSize)
    train(label, neuralPrefs, net, trainIter, testIter)
  }

  def trainBinaryFFN(label: String, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val net = FeedForward.create(neuralPrefs)
    val trainIter = new FeedForwardIterator(neuralPrefs.train.collect(), label, batchSize = neuralPrefs.minibatchSize)
    val testIter = new FeedForwardIterator(neuralPrefs.validation.collect(), label, batchSize = neuralPrefs.minibatchSize)
    train(label, neuralPrefs, net, trainIter, testIter)
  }

  def trainBinaryFFNBoW(label: String, neuralPrefs: NeuralPrefs, phrases: Array[String]): MultiLayerNetwork = {
    val net = FeedForward.createBoW(neuralPrefs, phrases.size)
    val trainIter = new FeedForwardIterator(neuralPrefs.train.collect(), label, batchSize = neuralPrefs.minibatchSize, phrases = phrases)
    val testIter = new FeedForwardIterator(neuralPrefs.validation.collect(), label, batchSize = neuralPrefs.minibatchSize, phrases = phrases)
    train(label, neuralPrefs, net, trainIter, testIter)
  }

  def train(label: String, neuralPrefs: NeuralPrefs, net: MultiLayerNetwork, trainIter: DataSetIterator, testIter: DataSetIterator): MultiLayerNetwork = {
    //    Log.r(s"Training $label ...")
    //    Log.r2(s"Training $label ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net.fit(trainIter)
      trainIter.reset()
      val eval = NeuralEvaluation(net, testIter, i, label)
      eval.log()
      testIter.reset()
    }

    net
  }

}
