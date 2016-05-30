package no.habitats.corpus.dl4j

import no.habitats.corpus.common.{Config, Log}
import org.bytedeco.javacpp.Pointer
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.collection.JavaConverters._

/**
  * Created by mail on 06.05.2016.
  */
object NeuralTrainer extends Serializable {
  case class NeuralResult(evaluations: Seq[NeuralEvaluation], net: MultiLayerNetwork)

  def train(name: String, label: String, neuralPrefs: NeuralPrefs, net: MultiLayerNetwork, trainIter: DataSetIterator, testIter: DataSetIterator): NeuralResult = {
    //    Log.r(s"Training $label ...")
    //    Log.r2(s"Training $label ...")
    Config.init()
    Log.v("Free bytes: " + Pointer.totalBytes())
    val total = trainIter.totalExamples()
    val batch: Int = trainIter.batch
    val totalEpochs: Int = Config.epoch.getOrElse(neuralPrefs.epochs)
    val eval: Seq[NeuralEvaluation] = for (epoch <- 0 until totalEpochs) yield {
      var c = 1
      while (trainIter.hasNext) {
        net.fit(trainIter.next())
        if ((c % 10) - 1 == 0) {
          val left = timeLeft(total = total, iteration = c, batch = batch, label = label, epoch = epoch, totalEpoch = totalEpochs)
          NeuralEvaluation(testIter.asScala.take(2).toTraversable, net, epoch, label, Some(neuralPrefs), Some(left)).logIntermediate(c)
          testIter.reset()
        }
        c += 1
      }
      val evaluation: NeuralEvaluation = NeuralEvaluation(testIter.asScala.toTraversable, net, epoch, label, Some(neuralPrefs))
      trainIter.reset()
      neuralPrefs.listener.reset
      testIter.reset()
      evaluation
    }
    NeuralResult(eval, net)
  }

  def timeLeft(total: Int, iteration: Int, batch: Int, label: String, epoch: Int, totalEpoch: Int): Int = {
    val labelIndex = Config.cats.toArray.sorted.indexOf(label)
    val duration = System.currentTimeMillis() - Config.start
    val articlesDone = (iteration * batch) + (total * epoch) + (labelIndex * total * totalEpoch)
    val articlesPerSecond = articlesDone / duration.toDouble

    val remainingBatch = total - (iteration * batch)
    val remainingLabels = Config.cats.size - labelIndex - 1
    val remainingEpochs = (totalEpoch - epoch - 1) + remainingLabels * totalEpoch
    val remainingArticles = remainingBatch + (remainingEpochs * total)

    val remaining = remainingArticles / articlesPerSecond

    remaining.toInt
  }
}
