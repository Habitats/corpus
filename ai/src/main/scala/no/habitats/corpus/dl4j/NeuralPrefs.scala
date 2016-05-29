package no.habitats.corpus.dl4j

case class NeuralPrefs(
                        learningRate: Double,
                        hiddenNodes: Int = 10,
                        minibatchSize: Int,
                        epochs: Int,
                        phrases: Option[Array[String]] = None
                      ) {

  lazy val listener: CorpusIterationListener = CorpusIterationListener()

  override def toString(): String = s"Hidden: $hiddenNodes - LR: $learningRate - Epochs: $epochs - Minibatch: $minibatchSize"
}
