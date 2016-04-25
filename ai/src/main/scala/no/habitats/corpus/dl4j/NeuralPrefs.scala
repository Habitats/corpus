package no.habitats.corpus.dl4j

import no.habitats.corpus.models.Article
import org.apache.spark.rdd.RDD

case class NeuralPrefs(
                        learningRate: Double,
                        hiddenNodes: Int,
                        train: RDD[Article],
                        validation: RDD[Article],
                        minibatchSize: Int = 50,
                        epochs: Int = 5,
                        histogram: Boolean = false
                      ) {
  override def toString(): String = s"Hidden: $hiddenNodes - LR: $learningRate - Epochs: $epochs - Minibatch: $minibatchSize - Histogram: $histogram"
}

