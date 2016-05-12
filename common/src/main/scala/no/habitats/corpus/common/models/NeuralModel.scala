package no.habitats.corpus.common.models

import no.habitats.corpus.common.{IPTC, W2VLoader}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.deeplearning4j.spark.util.MLLibUtil
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.ops.transforms.Transforms

case class NeuralModel(id: String, features: INDArray, iptc: Set[String])

