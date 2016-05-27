package no.habitats.corpus.common.dl4j

import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Log, TFIDF, W2VLoader}
import org.deeplearning4j.nn.conf.layers.GravesLSTM
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

import scala.collection.mutable

case class NeuralPredictor(net: MultiLayerNetwork, articles: Array[Article], label: String, modelType: Option[TFIDF]) {
  val featureDimensions: Int     = modelType.map(_.phrases.size).getOrElse(1000)
  val isRecurrent      : Boolean = net.getLayerWiseConfigurations.getConf(0).getLayer.isInstanceOf[GravesLSTM]

  def correct(log: Boolean = false): Array[Boolean] = {
    val res = if (isRecurrent) recurrentPrediction else feedforwardPrediction
    res.map { case (trueRes, falseRes) =>
      if (log) Log.v(s"Prediction for $label: $falseRes - $trueRes")
      trueRes > falseRes
    }
  }

  def feedforwardPrediction: Array[(Double, Double)] = {
    val features = Nd4j.create(articles.size, featureDimensions)
    val vector = modelType match {
      case None => articles.map(_.toDocumentVector)
      case Some(v) => articles.map(a => MLLibUtil.toVector(v.toVector(a)))
    }
    vector.zipWithIndex.foreach { case (v, i) => features.putRow(i, v) }
    val predicted = net.output(features, false)
    (for {i <- articles.indices} yield {
      val falseRes = predicted.getRow(i).getScalar(0).getDouble(0)
      val trueRes = predicted.getRow(i).getScalar(1).getDouble(0)
      (trueRes, falseRes)
    }).toArray
  }

  def recurrentPrediction: Array[(Double, Double)] = {
    // Filter out all ID's with matching vectors
    val numFeatures = articles.map(_.ann.size).max

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(articles.size, featureDimensions, numFeatures, 'f')
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.zeros(articles.size, numFeatures, 'f')
    val labelsMask = Nd4j.zeros(articles.size, numFeatures, 'f')

    for {i <- articles.indices} yield {
      val tokens: List[(Double, String)] = articles(i).ann.values
        // We want to preserve order
        .toSeq.sortBy(ann => ann.offset)
        .map(ann => (ann.tfIdf, ann.fb))
        .toList
      for (j <- tokens.indices) {
        val (tfidf, id) = tokens(j)
        val vector: INDArray = W2VLoader.fromId(id).get.mul(tfidf)
        features.put(Array(NDArrayIndex.point(i), NDArrayIndex.all(), NDArrayIndex.point(j)), vector)
        featureMask.putScalar(Array(i, j), 1.0)
      }
    }
    labelsMask.putScalar(Array(0, numFeatures - 1), 1.0)

    // Make the prediction, based on current features
    val predicted = net.output(features, false, featureMask, labelsMask)
    articles.indices.map { i =>
      val falseRes = predicted.getRow(i).getScalar(numFeatures - 1).getDouble(0)
      val trueRes = predicted.getRow(i).getScalar(numFeatures * 2 - 1).getDouble(0)
      (trueRes, falseRes)
    }.toArray
  }
}

object NeuralPredictor {
  val loadedModes: mutable.Map[String, Map[String, MultiLayerNetwork]] = mutable.Map()

  def predictAll(articles: Array[Article], models: Map[String, MultiLayerNetwork], modelType: Option[TFIDF]): Array[Article] = {
    val batchSize = 5000
    val batches = articles.size / batchSize
    (for {batchStart <- 0 until batches} yield {
      val batch = articles.slice(batchStart * batchSize, (batchStart + 1) * batchSize)
      val allPredictions: Map[String, Array[Boolean]] = models.map { case (label, model) => (label, NeuralPredictor(model, batch, label, modelType).correct()) }
      batch.zipWithIndex.map { case (article, articleIndex) => {
        val predicted = allPredictions.filter { case (label, res) => res(articleIndex) }.keySet
        article.copy(pred = predicted)
      }
      }
    }).reduce(_ ++ _)
  }

  def predict(article: Article, modelName: String): Set[String] = {
    val models = loadedModes.getOrElseUpdate(modelName, NeuralModelLoader.models(modelName))
    val modelType = if (modelName.toLowerCase.contains("bow")) Some(TFIDF.deserialize(modelName)) else None
    val predictors: Map[String, NeuralPredictor] = models.map { case (label, model) => (label, new NeuralPredictor(model, Array(article), label, modelType)) }
    val results: Set[String] = predictors.map { case (label, predictor) => s"$label: ${predictor.correct()}" }.toSet
    results
  }
}
