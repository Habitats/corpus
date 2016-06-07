package no.habitats.corpus.dl4j.networks

import java.util.concurrent.atomic.AtomicInteger

import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.dl4j.NeuralPrefs
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{GradientNormalization, MultiLayerConfiguration, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.ui.weights.HistogramIterationListener
import org.nd4j.linalg.lossfunctions.LossFunctions

object RNN {

  def create(neuralPrefs: NeuralPrefs): MultiLayerNetwork = {
    val conf: MultiLayerConfiguration = createConfig(2, neuralPrefs)
    build(neuralPrefs, conf)
  }

  def create(neuralPrefs: NeuralPrefs, coefficients: String): MultiLayerNetwork = {
    val net = NeuralModelLoader.load(createConfig(2, neuralPrefs), coefficients)
    net.setListeners(Array(neuralPrefs.listener) ++ (if (Config.histogram) Array(new HistogramIterationListener(2)) else Nil): _*)
    net.setUpdater(null)
    net
  }

  private def build(neuralPrefs: NeuralPrefs, conf: MultiLayerConfiguration): MultiLayerNetwork = {
    val net = new MultiLayerNetwork(conf)
    net.init()
    Log.v(s"Initialized network with ${net.numParams} params!")
    net.setListeners(Array(neuralPrefs.listener) ++ (if (Config.histogram) Array(new HistogramIterationListener(2)) else Nil): _*)
    net.setUpdater(null)
    net
  }

  def createConfig(output: Int, neuralPrefs: NeuralPrefs): MultiLayerConfiguration = {
    val vectorSize = 1000
    val hiddenNodes = Config.hidden1.getOrElse(neuralPrefs.hiddenNodes) // should not be less than a quarter of the input size

    Log.v(f"Count: ${Config.count} - $neuralPrefs")

    val i = new AtomicInteger(0)
    val conf = new NeuralNetConfiguration.Builder()
      .seed(Config.seed)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(Config.iterations.getOrElse(1))
      .updater(Updater.RMSPROP)
      .regularization(Config.l2.isDefined).l2(Config.l2.getOrElse(1e-5))
      .weightInit(WeightInit.XAVIER)
      .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue).gradientNormalizationThreshold(1.0)
      .learningRate(Config.learningRate.getOrElse(neuralPrefs.learningRate))
      .list()
      .layer(i.getAndIncrement(), new GravesLSTM.Builder()
        .nIn(vectorSize)
        .nOut(hiddenNodes)
        .name(s"$i")
        .activation(Config.activation.getOrElse("softsign"))
        .build())
      .layer(i.getAndIncrement(), new RnnOutputLayer.Builder()
        .nIn(hiddenNodes)
        .nOut(output)
        .name(s"$i")
        .activation("softmax")
        .lossFunction(LossFunctions.LossFunction.MCXENT)
        .build())
      .pretrain(false)
      .backprop(true)
      .build()
    conf
  }
}
