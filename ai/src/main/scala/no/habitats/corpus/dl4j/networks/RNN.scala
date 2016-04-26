package no.habitats.corpus.dl4j.networks

import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.dl4j.NeuralPrefs
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.deeplearning4j.ui.weights.HistogramIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

object RNN {

  def createBinary(neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    build(2, neuralPrefs)
  }

  def create(neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    build(17, neuralPrefs)
  }

  private def build(output: Int, neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val vectorSize = 1000
    val hiddenNodes = neuralPrefs.hiddenNodes // should not be less than a quarter of the input size
    val learningRate = neuralPrefs.learningRate

    Log.r(f"Count: ${Config.count} - $neuralPrefs")
    Log.r2(f"Count: ${Config.count} - $neuralPrefs")

    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1)
      .updater(Updater.RMSPROP)
      .regularization(true).l2(1e-5)
      .weightInit(WeightInit.XAVIER)
      .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue).gradientNormalizationThreshold(1.0)
      .learningRate(learningRate)
      .list()
      .layer(0, new GravesLSTM.Builder()
        .nIn(vectorSize)
        .nOut(hiddenNodes)
        .activation("softsign")
        .build())
      .layer(1, new GravesLSTM.Builder()
        .nIn(hiddenNodes)
        .nOut(hiddenNodes)
        .activation("softsign")
        .build())
      .layer(2, new RnnOutputLayer.Builder()
        .nIn(hiddenNodes)
        .nOut(output)
        .activation("softmax")
        .lossFunction(LossFunctions.LossFunction.MCXENT)
        .build())
      .pretrain(false)
      .backprop(true)
      .build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    Log.v(s"Initialized network with ${net.numParams} params!")
    net.setListeners(new ScoreIterationListener(1))
    if (neuralPrefs.histogram) {
      net.setListeners(new HistogramIterationListener(1))
    }
    net.setUpdater(null)
    net
  }
}
