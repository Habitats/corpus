package no.habitats.corpus.dl4j.multi

import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

object MultiLabelRNN {

  def create(): MultiLayerNetwork = {
    val vectorSize = 1000
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
        .nOut(17)
        .build()
      )
      .pretrain(false).backprop(true).build()
    val net = new MultiLayerNetwork(conf)
    net.init()
    net.setListeners(new ScoreIterationListener(1))
    net.setUpdater(null)
    net
  }
}
