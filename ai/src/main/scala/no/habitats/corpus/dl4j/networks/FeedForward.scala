package no.habitats.corpus.dl4j.networks

import java.util.concurrent.atomic.AtomicInteger

import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.dl4j.NeuralPrefs
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.ui.weights.HistogramIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

object FeedForward {

  def create(neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val numInputs = 1000
    val numOutputs = 2
    val firstLayer = Config.hidden1.getOrElse(700)
    val secondLayer = Config.hidden2.getOrElse(500)

    Log.rr(f"W2V - Count: ${Config.count} - $neuralPrefs")

    val i = new AtomicInteger(0)
    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .iterations(Config.iterations.getOrElse(1))
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .learningRate(Config.learningRate.getOrElse(neuralPrefs.learningRate))
      .updater(Updater.RMSPROP)
      .weightInit(WeightInit.XAVIER)
      .list()
      .layer(i.getAndIncrement(), new DenseLayer.Builder()
        .nIn(numInputs)
        .nOut(firstLayer)
        .name(s"$i")
        .activation("tanh")
        .build()
      )
      .layer(i.getAndIncrement(), new DenseLayer.Builder()
        .nIn(firstLayer)
        .nOut(secondLayer)
        .name(s"$i")
        .activation("tanh")
        .build()
      )
      .layer(i.getAndIncrement(), new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation("softmax")
        .nIn(secondLayer)
        .nOut(numOutputs)
        .name(s"$i")
        .build()
      )
      .pretrain(false)
      .backprop(true)
      .build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    Log.v(s"Initialized ${net.getLayers.length} layer Feedforward net [${net.numParams()} params] - ${neuralPrefs}")
    net.setListeners(Array(neuralPrefs.listener) ++ (if (Config.histogram) Array(new HistogramIterationListener(2)) else Nil): _*)

    net
  }

  def createBoW(neuralPrefs: NeuralPrefs, numInputs: Int): MultiLayerNetwork = {
    val numOutputs = 2
    val firstLayer = Config.hidden1.getOrElse(1000)
    val secondLayer = Config.hidden2.getOrElse(300)
    val thirdLayer = Config.hidden3.getOrElse(200)

    Log.rr(f"BoW - Count: ${Config.count} - $neuralPrefs")

    val i = new AtomicInteger(0)
    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .iterations(Config.iterations.getOrElse(1))
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .learningRate(Config.learningRate.getOrElse(neuralPrefs.learningRate))
      .regularization(Config.l2.isDefined).l2(Config.l2.getOrElse(1e-3))
      .updater(Updater.RMSPROP)
      .weightInit(WeightInit.XAVIER)

    val layers = conf.list()
      .layer(i.getAndIncrement(), new DenseLayer.Builder()
        .nIn(numInputs)
        .nOut(firstLayer)
        .activation("tanh")
        .name(s"$i")
        .build()
      )
//      .layer(i.getAndIncrement(), new DenseLayer.Builder()
//        .nIn(firstLayer)
//        .nOut(secondLayer)
//        .activation("tanh")
//        .name(s"$i")
//        .build()
//      )
//      .layer(i.getAndIncrement(), new DenseLayer.Builder()
//        .nIn(secondLayer)
//        .nOut(thirdLayer)
//        .activation("tanh")
//        .name(s"$i")
//        .build()
//      )
      .layer(i.getAndIncrement(), new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation("softmax")
        .nIn(firstLayer)
        .nOut(numOutputs)
        .name(s"$i")
        .build()
      )

    val net = new MultiLayerNetwork(layers.pretrain(false).backprop(true).build())
    net.init()
    Log.v(s"Initialized ${net.getLayers.length} layer Feedforward net with ${net.numParams()} params!")
    net.setListeners(Array(neuralPrefs.listener) ++ (if (Config.histogram) Array(new HistogramIterationListener(2)) else Nil): _*)

    net
  }

}
