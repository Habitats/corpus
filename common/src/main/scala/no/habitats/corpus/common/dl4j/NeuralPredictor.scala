package no.habitats.corpus.common.dl4j

import no.habitats.corpus.common._
import no.habitats.corpus.common.models.{Article, CorpusDataset}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.deeplearning4j.nn.conf.layers.GravesLSTM
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

case class NeuralPredictor(net: MultiLayerNetwork, articles: Array[Article], label: String, tfidf: TFIDF) {
  val featureDimensions: Int     = if (tfidf.name.contains("bow")) tfidf.phrases.size else 1000
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
    val vector = articles.map(a => CorpusDataset.toINDArray(tfidf, a))

    vector.zipWithIndex.foreach { case (v, i) => features.putRow(i, v) }
    val predicted = net.output(features, false)
    (for {i <- articles.indices} yield {
      val falseRes = predicted.getRow(i).getScalar(0).getDouble(0)
      val trueRes = predicted.getRow(i).getScalar(1).getDouble(0)
      (trueRes, falseRes)
    }).toArray
  }

  def recurrentPrediction: Array[(Double, Double)] = {
    val cutoff = 100
    // Filter out all ID's with matching vectors
    val numFeatures = Math.min(articles.map(_.ann.size).max, cutoff)

    // [miniBatchSize, inputSize, timeSeriesLength]
    val features = Nd4j.create(Array(articles.size, featureDimensions, numFeatures), 'f')
    // [miniBatchSize, timeSeriesLength]
    val featureMask = Nd4j.create(Array(articles.size, numFeatures), 'f')
    val labelsMask = Nd4j.create(Array(articles.size, numFeatures), 'f')

    for (i <- articles.indices) yield {
      val annotations = articles(i).ann.values
        // We want to preserve order
        .toSeq.sortBy(ann => ann.offset)
        .map(_.fb)
        .map(CorpusDataset.wordVector)
        .toList
        .take(cutoff)
      for (j <- annotations.indices) {
        features.put(Array(NDArrayIndex.point(i), NDArrayIndex.all(), NDArrayIndex.point(j)), annotations(j))
        featureMask.putScalar(Array(i, j), 1.0)
      }
      labelsMask.putScalar(Array(i, annotations.size - 1), 1.0)
    }

    // Make the prediction, based on current features
    val predicted = net.output(features, false, featureMask, labelsMask)
    articles.indices.map { i =>
      val numAnnotations = Math.min(articles(i).ann.size, cutoff)
      val falseRes = predicted.getRow(i).getScalar(numAnnotations - 1).getDouble(0)
      val trueRes = predicted.getRow(i).getScalar(numFeatures + numAnnotations - 1).getDouble(0)
      (trueRes, falseRes)
    }.toArray
  }
}

object NeuralPredictor {
  val loadedModes: mutable.Map[String, Map[String, NeuralModel]] = mutable.Map()

  /** For every partition, split partition into minibatches, predict minibatches, then combine */
  def predict(articles: RDD[Article], models: Map[String, NeuralModel], tfidf: TFIDF): RDD[Article] = {
    val filtered = articles.collect.map(_.filterAnnotation(a => tfidf.contains(a.id) && W2VLoader.contains(a.fb))).filter(_.ann.nonEmpty)
    CorpusContext.sc.parallelize(predictPartition(models, filtered, tfidf))
  }
  def predictPartition(models: Map[String, NeuralModel], partition: Array[Article], tfidf: TFIDF): Array[Article] = {
    val batchSize = 500
    val batches = (partition.size.toDouble / batchSize).ceil.toInt
    val predictedBatches: IndexedSeq[Array[Article]] = for {batchStart <- 0 until batches} yield {
      val batch: Array[Article] = partition.slice(batchStart * batchSize, (batchStart + 1) * batchSize).toArray
      val allPredictions: Map[String, Array[Boolean]] = models.map { case (label, model) => (label, NeuralPredictor(model.network, batch, label, tfidf).correct()) }
      batch.zipWithIndex.map { case (article, articleIndex) => {
        val predicted = allPredictions.filter { case (label, res) => res(articleIndex) }.keySet
        article.copy(pred = predicted)
      }
      }
    }
    predictedBatches.fold(Array())(_ ++ _)
  }

  def predict(article: Article, modelName: String): Set[String] = {
    val tag = "baseline"
    val models = loadedModes.getOrElseUpdate(modelName, NeuralModelLoader.models(Config.modelDir(modelName, tag)))
    val tfidf = TFIDF.deserialize(Config.modelDir(modelName, tag))
    val predictors: Map[String, NeuralPredictor] = models.map { case (label, model) => (label, new NeuralPredictor(model.network, Array(article), label, tfidf)) }
    val results: Set[String] = predictors.map { case (label, predictor) => f"$label%-44s: ${predictor.correct().head}" }.toSet
    results
  }
}
