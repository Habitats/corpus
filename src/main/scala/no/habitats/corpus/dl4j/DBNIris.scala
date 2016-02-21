package no.habitats.corpus.dl4j

import java.util.Random

import no.habitats.corpus.Log
import org.deeplearning4j.datasets.iterator.impl.IrisDataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.api.{Layer, OptimizationAlgorithm}
import org.deeplearning4j.nn.conf.layers.{OutputLayer, RBM}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.params.DefaultParamInitializer
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions


/**
  * Created by mail on 17.02.2016.
  */
object DBNIris {
  def main(args: Array[String]) = {
    // customizing params
    Nd4j.MAX_SLICES_TO_PRINT = -1
    Nd4j.MAX_ELEMENTS_PER_SLICE = -1

    val numRows = 4
    val numColumns = 1
    val outputNum = 3
    val numSamples = 150 // total number of input examples
    val batchSize = 150 // how many examples to fetch at each step
    val iterations = 200
    val splitTrainNum = (batchSize * 0.8).toInt
    val seed = 123
    val listenerFreq = 1

    Log.v("Load data ...")
    val iter = new IrisDataSetIterator(batchSize, numSamples)
    val next = iter.next()
    next.shuffle()
    next.normalizeZeroMeanZeroUnitVariance() // normalizes input, better suited for gradient decent etc

    Log.v("Split data ...")
    val testAndTrain = next.splitTestAndTrain(splitTrainNum, new Random(seed))
    val train = testAndTrain.getTrain
    val test = testAndTrain.getTest
    Nd4j.ENFORCE_NUMERICAL_STABILITY = true

    Log.v("Build model ...")
    val conf = new NeuralNetConfiguration.Builder()
      .seed(seed)
      .iterations(iterations)
      .learningRate(1e-6f)
      .optimizationAlgo(OptimizationAlgorithm.CONJUGATE_GRADIENT) // backpropr to calculate gradients
      .l1(1e-1).regularization(true).l2(2e-4) // regularization fights overfitting
      .useDropConnect(true) // helps the net generazile from training data by randomly cancelling out the interlayer edges between nodes
      .list(2) // number of layers (0-indexed)
      .layer(0,
      new RBM.Builder(RBM.HiddenUnit.RECTIFIED, RBM.VisibleUnit.GAUSSIAN) // apply gaussian white noise to normalize the distribution of continious data
        .nIn(numRows * numColumns) // # input nodes. rows = features
        .nOut(outputNum) // # labels
        .weightInit(WeightInit.XAVIER) // which algoritm to use on the randomly generated initial weights. xavier keeps weights from becoming too small/big
        .activation("relu") // rectified linear transform
        .lossFunction(LossFunctions.LossFunction.RMSE_XENT) // Root Mean Square Error, useful for penalizing huge errors. Cross-entropy assumes predicted valuesa are in the range 0-1. the loss function calculates the error produced by the weights of the model
        .updater(Updater.ADAGRAD)
        .dropOut(0.5)
        build()
    ).layer(1,
      new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .nIn(outputNum)
        .nOut(outputNum)
        .activation("softmax")
        .build()
    ).build()

    val model = new MultiLayerNetwork(conf)
    model.init()
    model.setListeners(new ScoreIterationListener(listenerFreq)) // an iterationListener monitors the iterations and reacts to what is happening

    Log.v("Train model ...")
    model.fit(train)

    Log.v("Evaluate weights ...")
    for {
      layer <- model.getLayers
      w = layer.getParam(DefaultParamInitializer.WEIGHT_KEY)
    } yield {
      Log.v("Weights: " + w)
    }

    Log.v("Evalutate model ...")
    val eval = new Evaluation(outputNum)
    val output = model.output(test.getFeatureMatrix, Layer.TrainingMode.TEST)
    for {i <- 0 until output.rows} yield {
      val actual = train.getLabels.getRow(i).toString.trim
      val predicted = output.getRow(i).toString.trim
      Log.v(f"Actual: $actual - Predicted: $predicted")
    }
    eval.eval(test.getLabels, output)
    Log.v(eval.stats)
    Log.v("Finished examples!")
  }
}
