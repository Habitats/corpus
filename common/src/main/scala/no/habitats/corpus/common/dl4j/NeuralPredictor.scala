package no.habitats.corpus.common.dl4j

import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, Log, TFIDF, W2VLoader}
import org.deeplearning4j.nn.conf.layers.GravesLSTM
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.{INDArrayIndex, NDArrayIndex}

import scala.collection.mutable

case class NeuralPredictor(net: MultiLayerNetwork, article: Article, label: String, modelType: Option[TFIDF]) {
  val featureDimensions: Int     = modelType.map(_.phrases.size).getOrElse(1000)
  val isRecurrent      : Boolean = net.getLayerWiseConfigurations.getConf(0).getLayer.isInstanceOf[GravesLSTM]

  def correct(log: Boolean = false): Boolean = {
    val (trueRes, falseRes) = if (isRecurrent) recurrentPrediction else feedforwardPrediction
    if (log) Log.v(s"Prediction for $label: $falseRes - $trueRes")
    trueRes > falseRes
  }

  def feedforwardPrediction: (Double, Double) = {
    val features = Nd4j.create(1, featureDimensions)
    val vector = modelType match {
      case None => article.toDocumentVector
      case Some(v) => MLLibUtil.toVector(v.toVector(article))
    }
    features.putRow(0, vector)
    val predicted = net.output(features, false)
    val falseRes = predicted.getScalar(0).getDouble(0)
    val trueRes = predicted.getScalar(1).getDouble(0)
    (trueRes, falseRes)
  }

  def recurrentPrediction: (Double, Double) = {
    // Filter out all ID's with matching vectors
    val tokens: List[String] = article.ann.values
      .filter(_.fb != Config.NONE)
      .map(_.fb)
      .filter(W2VLoader.contains)
      .toList
    val numFeatures: Int = tokens.size

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(1, featureDimensions, numFeatures)
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.zeros(1, numFeatures)
    val labelsMask = Nd4j.zeros(1, numFeatures)

    for (j <- tokens.indices) {
      val vector = modelType match {
        case None => article.toDocumentVector
        case Some(v) => MLLibUtil.toVector(v.toVector(article))
      }
      val indices: Array[INDArrayIndex] = Array(NDArrayIndex.point(0), NDArrayIndex.all(), NDArrayIndex.point(j))
      features.put(indices, vector)
      featureMask.putScalar(Array(0, j), 1.0)
    }
    labelsMask.putScalar(Array(0, numFeatures - 1), 1.0)

    // Make the prediction, based on current features
    val predicted = net.output(features, false, featureMask, labelsMask)
    val falseRes = predicted.getScalar(numFeatures - 1).getDouble(0)
    val trueRes = predicted.getScalar(numFeatures * 2 - 1).getDouble(0)
    (trueRes, falseRes)
  }
}

object NeuralPredictor {
  val loadedModes: mutable.Map[String, Map[String, MultiLayerNetwork]] = mutable.Map()

  def predictAll(articles: Seq[Article], models: Map[String, MultiLayerNetwork], modelType: Option[TFIDF]): Seq[Article] = {
    for {article <- articles} yield predict(models, article, modelType)
  }

  def predict(models: Map[String, MultiLayerNetwork], article: Article, modelType: Option[TFIDF]): Article = {
    val predicted: Set[String] = models.filter { case (label, model) => NeuralPredictor(model, article, label, modelType).correct() }.keySet
    article.copy(pred = predicted)
  }

  def predict(article: Article, modelName: String): Set[String] = {
    val models = loadedModes.getOrElseUpdate(modelName, NeuralModelLoader.models(modelName))
    val modelType = if (modelName.toLowerCase.contains("bow")) Some(TFIDF.deserialize(modelName)) else None
    val predictors: Map[String, NeuralPredictor] = models.map { case (label, model) => (label, new NeuralPredictor(model, article, label, modelType)) }
    val results: Set[String] = predictors.map { case (label, predictor) => s"$label: ${predictor.correct()}" }.toSet
    results
  }
}
