package no.habitats.corpus.dl4j

import no.habitats.corpus.common.models.Article

case class NeuralPrefs(
                        learningRate: Double ,
                        hiddenNodes: Int = 10,
                        train: Array[Article],
                        validation: Array[Article],
                        minibatchSize: Int,
                        epochs: Int ,
                        phrases: Option[Array[String]] = None
                      ) {

  lazy val listener: CorpusIterationListener = CorpusIterationListener()

  override def toString(): String = s"Hidden: $hiddenNodes - LR: $learningRate - Epochs: $epochs - Minibatch: $minibatchSize"
}
