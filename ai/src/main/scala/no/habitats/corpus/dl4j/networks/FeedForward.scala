package no.habitats.corpus.dl4j.networks

import no.habitats.corpus.common.Config
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

class FeedForward {

  val learningRate = 0.005
  val numInputs = 2
  val numOutputs = 2
  val numHidden = 20

  def create(): MultiLayerNetwork = {
    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .iterations(1)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .learningRate(learningRate)
      .updater(Updater.NESTEROVS).momentum(0.9)
      .list(2)
      .layer(1, new DenseLayer.Builder()
        .nIn(numInputs)
        .nOut(numHidden)
        .weightInit(WeightInit.XAVIER)
        .activation("relu")
        .build()
      )
      .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
        .weightInit(WeightInit.XAVIER)
        .activation("softmax")
        .nIn(numHidden)
        .nOut(numOutputs)
        .build()
      )
      .pretrain(false)
      .backprop(true)
      .build()
    val model = new MultiLayerNetwork(conf)
    model.init()
    model.setListeners(new ScoreIterationListener(1))

    model
  }

}
