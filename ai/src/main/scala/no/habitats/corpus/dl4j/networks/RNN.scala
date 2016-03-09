package no.habitats.corpus.dl4j.networks

import no.habitats.corpus.{Config, Log}
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

object RNN {

  def createBinary(): MultiLayerNetwork = {
    build(2)
  }

  def create(): MultiLayerNetwork = {
    build(17)
  }

  private def build(output: Int): MultiLayerNetwork = {
    val vectorSize = 1000
    val hiddenNodes = 333 // should not be less than a quarter of the input size
    val learningRate = 0.05

    Log.r(f"Count: ${Config.count} - Hidden: $hiddenNodes - LR: $learningRate")
    Log.r2(f"Count: ${Config.count} - Hidden: $hiddenNodes - LR: $learningRate")

    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1)
      .updater(Updater.RMSPROP)
      .regularization(true).l2(1e-5)
      .weightInit(WeightInit.XAVIER)
      .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue).gradientNormalizationThreshold(1.0)
      .learningRate(learningRate)
      .list(2)
      .layer(0, new GravesLSTM.Builder()
        .nIn(vectorSize)
        .nOut(hiddenNodes)
        .activation("softsign")
        .build())
      .layer(1, new RnnOutputLayer.Builder()
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
    net.setListeners(new ScoreIterationListener(1))
    //    net.setListeners(new HistogramIterationListener(1))
    net.setUpdater(null)
    net
  }
}
