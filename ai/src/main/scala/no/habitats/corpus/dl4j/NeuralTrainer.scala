package no.habitats.corpus.dl4j

import no.habitats.corpus.common.{Config, Log}
import org.bytedeco.javacpp.Pointer
import org.deeplearning4j.datasets.iterator.DataSetIterator
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
    val total = trainIter.totalExamples()
    val batch: Int = trainIter.batch
    val totalEpochs: Int = Config.epoch.getOrElse(neuralPrefs.epochs)
    val resultFile = if (Config.parallelism > 1) {
      val r = s"train/$name/$label.txt"
      Log.toFile(Config.getArgs.toString, r)
      r
    } else s"train/$name.txt"

    val eval: Seq[NeuralEvaluation] = for (epoch <- 0 until totalEpochs) yield {
      var c = 1
      while (trainIter.hasNext) {
        net.fit(trainIter.next())
        if ((c % 10) - 1 == 0) {
          val left = timeLeft(totalTrainingSize = total, currentIteration = c, batch = batch, label = label, currentEpoch = epoch, totalEpoch = totalEpochs)
          NeuralEvaluation(testIter.asScala.take(2).toTraversable, net, epoch, label, Some(neuralPrefs), Some(left)).log(resultFile, c - 1)
          testIter.reset()
        }
        c += 1
      }
      val evaluation: NeuralEvaluation = NeuralEvaluation(testIter.asScala.toTraversable, net, epoch, label, Some(neuralPrefs))
      trainIter.reset()
      neuralPrefs.listener.reset()
      testIter.reset()
      evaluation
    }
    System.gc()
    NeuralResult(eval, net)
  }

  def timeLeft(totalTrainingSize: Int, currentIteration: Int, batch: Int, label: String, currentEpoch: Int, totalEpoch: Int): Int = {
    val labelIndex = if (Config.parallelism == 1) Config.cats.toArray.sorted.indexOf(label) else 0
    val totalLabels = if (Config.parallelism == 1) Config.cats.size else 1
    val duration = System.currentTimeMillis() - Config.start

    val articlesDoneBefore = totalEpoch * labelIndex * totalTrainingSize
    val articlesDoneBeforeCurrentLabel = currentEpoch * totalTrainingSize
    val articlesDoneBeforeCurrentIteration = currentIteration * batch
    val articlesDone = articlesDoneBefore + articlesDoneBeforeCurrentLabel + articlesDoneBeforeCurrentIteration

    val totalArticlesToDo = totalTrainingSize * totalLabels * totalEpoch
    val articlesRemaining = totalArticlesToDo - articlesDone

    val articleFrequency = articlesDone / duration.toDouble
    val remainingTime = articlesRemaining / articleFrequency

    remainingTime.toInt
  }
}
