package no.habitats.corpus.dl4j.networks

import java.util.concurrent.atomic.AtomicInteger

import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.dl4j.NeuralPrefs
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
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
    val hiddenNodes = Config.hidden1.getOrElse(neuralPrefs.hiddenNodes) // should not be less than a quarter of the input size
    val learningRate = Config.learningRate.getOrElse(neuralPrefs.learningRate)

    Log.resultHeader(f"Count: ${Config.count} - $neuralPrefs")

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
        .activation("softsign")
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
    val net = new MultiLayerNetwork(conf)
    net.init()
    Log.v(s"Initialized network with ${net.numParams} params!")
    val listeners = Array(neuralPrefs.listener)
    net.setListeners(Array(neuralPrefs.listener) ++ (if (Config.histogram) Array(new HistogramIterationListener(2)) else Nil): _*)
    net.setUpdater(null)
    net
  }
}
