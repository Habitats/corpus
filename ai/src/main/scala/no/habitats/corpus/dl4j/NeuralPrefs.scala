package no.habitats.corpus.dl4j

import no.habitats.corpus.common.models.Article

case class NeuralPrefs(
                        learningRate: Double = 0.05,
                        hiddenNodes: Int = 10,
                        train: Array[Article],
                        validation: Array[Article],
                        minibatchSize: Int = 50,
                        epochs: Int = 5,
                        histogram: Boolean = false,
                        phrases: Option[Array[String]] = None
                      ) {

  lazy val listener: CorpusIterationListener = CorpusIterationListener()

  override def toString(): String = s"Hidden: $hiddenNodes - LR: $learningRate - Epochs: $epochs - Minibatch: $minibatchSize - Histogram: $histogram"
}
