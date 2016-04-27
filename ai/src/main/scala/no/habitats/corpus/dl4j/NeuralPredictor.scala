package no.habitats.corpus.dl4j

import no.habitats.corpus.common.{Log, NeuralModelLoader, W2VLoader}
import no.habitats.corpus.models.{Annotation, Article}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.{INDArrayIndex, NDArrayIndex}

case class NeuralPredictor(net: MultiLayerNetwork, article: Article, label: String) {

  def correct(): Boolean = {

    // Filter out all ID's with matching vectors
    val tokens = article.ann.values
      .filter(_.fb != Annotation.NONE)
      .map(_.fb)
      .filter(W2VLoader.contains)
      .toList

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(1, 1000, tokens.size)
    val labels = Nd4j.create(1, 2, tokens.size)
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.zeros(1, tokens.size)
    val labelsMask = Nd4j.zeros(1, tokens.size)

    // Set w2v vectors as features
    for (j <- tokens.indices) {
      val vector: INDArray = W2VLoader.fromId(tokens(j)).get
      val indices: Array[INDArrayIndex] = Array(NDArrayIndex.point(0), NDArrayIndex.all(), NDArrayIndex.point(j))
      features.put(indices, vector)
      featureMask.putScalar(Array(0, j), 1.0)
    }
    labelsMask.putScalar(Array(0, tokens.size - 1), 1.0)

    // Make the prediction, based on current features
    val predicted = net.output(features, false, featureMask, labelsMask)
    val falseRes = predicted.getScalar(tokens.size - 1).getDouble(0)
    val trueRes = predicted.getScalar(tokens.size * 2 - 1).getDouble(0)
    net.score()

    Log.v(s"Prediction for $label: $falseRes - $trueRes")

    trueRes > falseRes
  }
}

object NeuralPredictor {

  def predict(article: Article): Set[String] = {
    val predictors: Map[String, NeuralPredictor] = NeuralModelLoader.bestModels("ffa").map { case (label, model) => (label, new NeuralPredictor(model, article, label)) }
    val results: Set[String] = predictors.map { case (label, predictor) => s"$label: ${predictor.correct()}" }.toSet
    results
  }
}
