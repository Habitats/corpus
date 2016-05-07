package no.habitats.corpus.dl4j

import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

/**
  * Created by mail on 06.05.2016.
  */
object NeuralTrainer {
  def train(label: String, neuralPrefs: NeuralPrefs, net: MultiLayerNetwork, trainIter: DataSetIterator, testIter: DataSetIterator): MultiLayerNetwork = {
    //    Log.r(s"Training $label ...")
    //    Log.r2(s"Training $label ...")
    for (i <- 0 until neuralPrefs.epochs) {
      net.fit(trainIter)
      trainIter.reset()
      val eval = NeuralEvaluation(net, testIter, i, label)
      eval.log()
      testIter.reset()
    }

    net
  }

}
