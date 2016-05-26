package no.habitats.corpus.dl4j

import no.habitats.corpus.common.Log
import org.bytedeco.javacpp.Pointer
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.collection.JavaConverters._

/**
  * Created by mail on 06.05.2016.
  */
object NeuralTrainer extends Serializable{
  def train(label: String, neuralPrefs: NeuralPrefs, net: MultiLayerNetwork, trainIter: DataSetIterator, testIter: DataSetIterator): MultiLayerNetwork = {
    //    Log.r(s"Training $label ...")
    //    Log.r2(s"Training $label ...")
    Log.v("Free bytes: " + Pointer.totalBytes())
    var c = 0
    for (i <- 0 until neuralPrefs.epochs) {
      while (trainIter.hasNext) {
        Log.v("1")
        net.fit(trainIter.next())
        Log.v("2")
        if (c % 10 == 0) {
          Log.v("3")
          val evaluation: NeuralEvaluation = NeuralEvaluation(net, testIter.asScala.take(2), i, label, Some(neuralPrefs))
          Log.v("4")
          evaluation.logv(label, c)
          Log.v("5")
          testIter.reset()
        }
        c += 1
      }
      trainIter.reset()
      NeuralEvaluation(net, testIter.asScala, i, label, Some(neuralPrefs)).log()
      neuralPrefs.listener.reset
      testIter.reset()
    }
    net
  }
}
