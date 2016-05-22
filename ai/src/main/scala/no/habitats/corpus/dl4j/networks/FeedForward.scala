package no.habitats.corpus.dl4j.networks

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
    val firstLayer = 700
    val secondLayer = 500

    Log.rr(f"W2V - Count: ${Config.count} - $neuralPrefs")

    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .iterations(1)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .learningRate(neuralPrefs.learningRate)
      .updater(Updater.RMSPROP)
      .weightInit(WeightInit.XAVIER)
      .list()
      .layer(0, new DenseLayer.Builder()
        .nIn(numInputs)
        .nOut(firstLayer)
        .activation("tanh")
        .build()
      )
      .layer(1, new DenseLayer.Builder()
        .nIn(firstLayer)
        .nOut(secondLayer)
        .activation("tanh")
        .build()
      )
      .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation("softmax")
        .nIn(secondLayer)
        .nOut(numOutputs)
        .build()
      )
      .pretrain(false)
      .backprop(true)
      .build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    Log.v(s"Initialized ${net.getLayers.length} layer Feedforward net [${net.numParams()} params] - ${neuralPrefs}")
    net.setListeners(neuralPrefs.listener)
    if (Config.histogram) {
      net.setListeners(new HistogramIterationListener(2))
    }

    net
  }

  def createBoW(neuralPrefs: NeuralPrefs, numInputs: Int): MultiLayerNetwork = {
    val numOutputs = 2
    val firstLayer = 1000
    val secondLayer = 700
    val thirdLayer = 500

    Log.rr(f"BoW - Count: ${Config.count} - $neuralPrefs")

    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .iterations(1)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .learningRate(neuralPrefs.learningRate)
      .regularization(false)
      .updater(Updater.RMSPROP)
      .weightInit(WeightInit.XAVIER)
      .list()
      .layer(0, new DenseLayer.Builder()
        .nIn(numInputs)
        .nOut(firstLayer)
        .activation("tanh")
        .name("0")
        .build()
      )
      .layer(1, new DenseLayer.Builder()
        .nIn(firstLayer)
        .nOut(secondLayer)
        .activation("tanh")
        .name("1")
        .build()
      )
      .layer(2, new DenseLayer.Builder()
        .nIn(secondLayer)
        .nOut(thirdLayer)
        .activation("tanh")
        .name("2")
        .build()
      )
      .layer(3, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation("softmax")
        .nIn(thirdLayer)
        .nOut(numOutputs)
        .name("3")
        .build()
      )
      .pretrain(false)
      .backprop(true)
      .build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    Log.v(s"Initialized ${net.getLayers.length} layer Feedforward net with ${net.numParams()} params!")
    net.setListeners(neuralPrefs.listener)
    if (Config.histogram) {
      net.setListeners(new HistogramIterationListener(1))
    }

    net
  }

}
