package no.habitats.corpus.dl4j

import no.habitats.corpus.common.{Log, W2VLoader}
import no.habitats.corpus.models.{Annotation, Article}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.{INDArrayIndex, NDArrayIndex}

case class NeuralPredictor(net: MultiLayerNetwork, article: Article, label: String) {
  def correct(): Boolean = {

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(1, 1000, article.ann.size)
    val labels = Nd4j.create(1, 2, article.ann.size)
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.zeros(1, article.ann.size)
    val labelsMask = Nd4j.zeros(1, article.ann.size)

    // Filter out all ID's with matching vectors
    val tokens = article.ann.values
      .filter(_.fb != Annotation.NONE)
      .map(_.fb)
      .filter(W2VLoader.contains)
      .toList

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
    val res = predicted.getScalar(tokens.size - 1)

    Log.v(s"Prediction for $label: $res")

    ???
  }
}
