package no.habitats.corpus.dl4j.ex

import no.habitats.corpus.Log
import org.deeplearning4j.datasets.iterator.impl.IrisDataSetIterator
import org.deeplearning4j.nn.api.{Layer, OptimizationAlgorithm}
import org.deeplearning4j.nn.conf.layers.RBM
import org.deeplearning4j.nn.conf.layers.RBM.{HiddenUnit, VisibleUnit}
import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.layers.factory.LayerFactories
import org.deeplearning4j.nn.params.DefaultParamInitializer
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions

object RBMIris {
  def main(args: Array[String]) = {
    Nd4j.MAX_SLICES_TO_PRINT = -1
    Nd4j.MAX_ELEMENTS_PER_SLICE = -1
    Nd4j.ENFORCE_NUMERICAL_STABILITY = true

    val numRows = 4
    val numColumns = 1
    val outputNum = 10
    val numSamples = 150
    val batchSize = 150
    val iterations = 100
    val seed = 123
    val listenerFreq = iterations / 2

    Log.v("Load data ...")
    val iter = new IrisDataSetIterator(batchSize, numSamples)
    val iris = iter.next()
    iris.normalizeZeroMeanZeroUnitVariance()

    Log.v("Build model ...")
    val conf = new NeuralNetConfiguration.Builder()
      .regularization(true)
      .miniBatch(true)
      .layer(new RBM.Builder().l2(1e-1).l1(1e-3)
        .nIn(numRows * numColumns)
        .nOut(outputNum)
        .activation("relu")
        .weightInit(WeightInit.RELU)
        .lossFunction(LossFunctions.LossFunction.RECONSTRUCTION_CROSSENTROPY).k(3)
        .hiddenUnit(HiddenUnit.RECTIFIED).visibleUnit(VisibleUnit.GAUSSIAN)
        .updater(Updater.ADAGRAD).gradientNormalization(GradientNormalization.ClipL2PerLayer)
        .build()
      )
      .seed(seed)
      .iterations(iterations)
      .learningRate(1e-3)
      .optimizationAlgo(OptimizationAlgorithm.LBFGS)
      .build()

    val model: Layer = LayerFactories.getFactory(conf.getLayer).create(conf)
    model.setListeners(new ScoreIterationListener(listenerFreq))

    Log.v("Evaluate weights ...")
    val w = model.getParam(DefaultParamInitializer.WEIGHT_KEY)
    Log.v("Weights: " + w)
    Log.v("Scaling the dataset")
    iris.scale()
    Log.v("Traing model ...")
    for (i <- 0 until 20) {
      Log.v(f"Epoch $i")
      model.fit(iris.getFeatureMatrix)
    }
  }
}
