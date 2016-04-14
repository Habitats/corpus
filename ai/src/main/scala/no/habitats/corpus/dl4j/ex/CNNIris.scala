package no.habitats.corpus.dl4j.ex

import java.util.Random

import no.habitats.corpus.common.Log
import org.deeplearning4j.datasets.iterator.impl.IrisDataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.setup.ConvolutionLayerSetup
import org.deeplearning4j.nn.conf.layers.{ConvolutionLayer, OutputLayer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.params.DefaultParamInitializer
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions

object CNNIris {
  def main(args: Array[String]) = {
    val numRows = 2
    val numColumns = 2
    val nChannels = 1
    val outputNum = 3
    val numSamples = 150
    val batchSize = 150
    val iterations = 10
    val splitTrainNum = 100
    val seed = 123
    val listenerFreq = 1

    Log.v("Load data ...")
    val irisIter = new IrisDataSetIterator(batchSize, numSamples)
    val iris = irisIter.next()
    iris.normalizeZeroMeanZeroUnitVariance()
    Log.v("Loaded " + iris.labelCounts() + " labels!")
    Nd4j.shuffle(iris.getFeatureMatrix, new Random(seed), 1)
    Nd4j.shuffle(iris.getLabels, new Random(seed), 1)
    val trainTest = iris.splitTestAndTrain(splitTrainNum, new Random(seed))
    val builder = new NeuralNetConfiguration.Builder()
      .seed(seed)
      .iterations(iterations)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .list(2)
      .layer(0,
        new ConvolutionLayer.Builder(1, 1)
          .nIn(nChannels)
          .nOut(1000)
          .activation("relu")
          .weightInit(WeightInit.RELU)
          .build())
      .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .nOut(outputNum)
        .weightInit(WeightInit.XAVIER)
        .activation("softmax")
        .build())
      .backprop(true).pretrain(false)

    new ConvolutionLayerSetup(builder, numRows, numColumns, nChannels)
    val conf = builder.build()

    Log.v("Build model ...")
    val model = new MultiLayerNetwork(conf)
    model.init()
    model.setListeners(new ScoreIterationListener(listenerFreq))

    Log.v("Train model ...")
    model.fit(trainTest.getTrain)

    Log.v("Evalutate weights ...")
    for {layer <- model.getLayers} yield {
      val w = layer.getParam(DefaultParamInitializer.WEIGHT_KEY)
      Log.v("Weights: " + w)
    }

    Log.v("Evalutate model ...")
    Log.v("Training on " + trainTest.getTest.labelCounts)
    val eval = new Evaluation(outputNum)
    val output = model.output(trainTest.getTest.getFeatureMatrix)
    eval.eval(trainTest.getTest.getLabels, output)
    Log.v(eval.stats)
  }
}
