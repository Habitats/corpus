package no.habitats.corpus.dl4j

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.dl4j.networks.{CorpusIterator, RNN}
import no.habitats.corpus.models.Article
import no.habitats.corpus.spark.RddFetcher
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.deeplearning4j.datasets.iterator.AsyncDataSetIterator
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j

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
    val vec = sc.textFile(Config.dataPath + "nyt/fb_w2v_0.5.txt")
      .map(_.split(", "))
      .filter(arr => filter.isEmpty || filter.contains(arr(0)))
      .map(arr => (arr(0), arr.toSeq.slice(1, arr.length).map(_.toFloat).toArray))
      .map(arr => {
        val vector = Nd4j.create(arr._2)
        val id = arr._1
        (id, vector)
      })
      .collect() // this takes a long time
      .toMap

    vec
  }

  def trainSparkMultiLabelRNN(label: Option[String] = None): MultiLayerNetwork = {
    val nEpochs = 5
    var net = label match {
      case None => RNN.create()
      case _ => RNN.createBinary()
    }
    val sparkNetwork = new SparkDl4jMultiLayer(sc, net)

    val trainIter: List[DataSet] = new CorpusIterator(train, label).asScala.toList
    val testIter = new AsyncDataSetIterator(new CorpusIterator(test, label))
    val rddTrain: JavaRDD[DataSet] = sc.parallelize(trainIter)

    Log.v("Starting training ...")
    for (i <- 0 until nEpochs) {
      net = sparkNetwork.fitDataSet(rddTrain)
      val eval = NeuralEvaluation(net, testIter, i)
      eval.log()
      testIter.reset()
    }

    net
  }

  def trainMultiLabelRNN(label: Option[String] = None): MultiLayerNetwork = {
    val nEpochs = 5
    val net = label match {
      case None => RNN.create()
      case _ => RNN.createBinary()
    }

    val trainIter = new AsyncDataSetIterator(new CorpusIterator(train, label))
    val testIter = new AsyncDataSetIterator(new CorpusIterator(test, label))
    Log.r(s"Training ${label.mkString(", ")} ...")
    Log.r2(s"Training ${label.mkString(", ")} ...")
    for (i <- 0 until nEpochs) {
      net.fit(trainIter)
      trainIter.reset()
      val eval = NeuralEvaluation(net, testIter, i)
      eval.log()
      testIter.reset()
    }

    net
  }
}
