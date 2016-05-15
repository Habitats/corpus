package no.habitats.corpus.dl4j.networks

import no.habitats.corpus.common.Config
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.setup.ConvolutionLayerSetup
import org.deeplearning4j.nn.conf.layers.{ConvolutionLayer, OutputLayer, SubsamplingLayer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

object CNN {

  val numRows       = 2
  val numColumns    = 2
  val nChannels     = 1
  val outputNum     = 3
  val numSamples    = 150
  val batchSize     = 150
  val iterations    = 10
  val splitTrainNum = 100
  val listenerFreq  = 1
  val stride        = Array(1, 1)
  val kernel        = Array(3, 1)

  def create(): MultiLayerNetwork = {
    val builder = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .iterations(iterations)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .regularization(true).l2(1e-5)
      .list()
      .layer(0, new ConvolutionLayer.Builder(1, 1)
        .nIn(nChannels)
        .nOut(1000)
        .activation("relu")
        .dropOut(0.5)
        .weightInit(WeightInit.RELU)
        .build())
      .layer(1, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX, kernel, stride)
        .build()
      )
      .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .nOut(outputNum)
        .weightInit(WeightInit.XAVIER)
        .activation("softmax")
        .build())
      .pretrain(false)
      .backprop(true)

    new ConvolutionLayerSetup(builder, numRows, numColumns, nChannels)
    val conf = builder.build()

    val model = new MultiLayerNetwork(conf)
    model.init()
    model.setListeners(new ScoreIterationListener(listenerFreq))
    model
  }
}
