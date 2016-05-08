package no.habitats.corpus.dl4j.networks

import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.dl4j.{CorpusIterationListener, NeuralPrefs}
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.deeplearning4j.ui.weights.HistogramIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

object FeedForward {

  def create(neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val numInputs = 1000
    val numOutputs = 2
    val firstLayer = 700
    val secondLayer = 500

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
    Log.r(s"Initialized ${net.getLayers.length} layer Feedforward net with ${net.numParams()} params!")
    net.setListeners(CorpusIterationListener())
    if (neuralPrefs.histogram) {
      net.setListeners(new HistogramIterationListener(1))
    }

    net
  }

  def createBoW(neuralPrefs: NeuralPrefs, numInputs: Int): MultiLayerNetwork = {
    val numOutputs = 2
    val firstLayer = (numInputs * 0.5).toInt

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
      .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation("softmax")
        .nIn(firstLayer)
        .nOut(numOutputs)
        .build()
      )
      .pretrain(false)
      .backprop(true)
      .build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    Log.r(s"Initialized ${net.getLayers.length} layer Feedforward net with ${net.numParams()} params!")
    net.setListeners(new ScoreIterationListener(1))
    if (neuralPrefs.histogram) {
      net.setListeners(new HistogramIterationListener(1))
    }

    net
  }

}
