package no.habitats.corpus.common.models

import org.nd4j.linalg.api.ndarray.INDArray

case class NeuralModel(id: String, features: INDArray, iptc: Set[String])

