package no.habitats.corpus.dl4j

import no.habitats.corpus.common.Log
import org.bytedeco.javacpp.Pointer
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.collection.JavaConverters._

/**
  * Created by mail on 06.05.2016.
  */
object NeuralTrainer {
  def train(label: String, neuralPrefs: NeuralPrefs, net: MultiLayerNetwork, trainIter: DataSetIterator, testIter: DataSetIterator): MultiLayerNetwork = {
    //    Log.r(s"Training $label ...")
    //    Log.r2(s"Training $label ...")
    Log.v("Free bytes: " + Pointer.totalBytes())
    var c = 0
    for (i <- 0 until neuralPrefs.epochs) {
      while (trainIter.hasNext) {
        net.fit(trainIter.next())
        c += 1
        if (c % 10 == 0) {
          val evaluation: NeuralEvaluation = NeuralEvaluation(net, testIter.asScala.take(2), i, label, Some(neuralPrefs))
          evaluation.logv(c)
          testIter.reset()
        }
      }
      trainIter.reset()
      NeuralEvaluation(net, testIter.asScala, i, label, Some(neuralPrefs)).log()
      testIter.reset()
    }
    net
  }
}
