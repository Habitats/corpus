package no.habitats.corpus.dl4j

import no.habitats.corpus.common.dl4j.NeuralModelLoader
import no.habitats.corpus.common.models.CorpusDataset
import no.habitats.corpus.common.{Config, Log}
import org.deeplearning4j.datasets.iterator.DataSetIterator
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object NeuralTrainer {

  case class IteratorPrefs(label: String, training: CorpusDataset, validation: CorpusDataset)
  case class NeuralResult(evaluations: Seq[NeuralEvaluation])

  def trainLabel(name: String, label: String, neuralPrefs: NeuralPrefs, net: MultiLayerNetwork, trainIter: DataSetIterator, testIter: DataSetIterator): NeuralResult = {
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

    val evals: Seq[NeuralEvaluation] = for (epoch <- 0 until totalEpochs) yield {
      var c = 1
      while (trainIter.hasNext) {
        net.fit(trainIter.next())
        if (((c * trainIter.batch) % 10000) - 1 == 0) {
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
      NeuralModelLoader.save(net, label, Config.count, name + s"_e$epoch")
      evaluation
    }
    System.gc()
    NeuralResult(evals)
  }

  def trainNetwork(validation: CorpusDataset, training: (String) => CorpusDataset, name: String, minibatchSize: Seq[Int], learningRate: Seq[Double], tp: (NeuralPrefs, IteratorPrefs) => NeuralResult) = {
    val resultFile = s"train/$name.txt"
    Log.toFile("", resultFile)
    Log.toFile("", resultFile)
    Log.toFile("", resultFile)
    Log.toFile("Model: " + name + " - Args: " + Config.getArgs, resultFile)
    Log.toFile("", resultFile)
    if (Config.parallelism > 1) parallel(validation, training(""), name, minibatchSize, learningRate, tp, Config.parallelism)
    else sequential(validation, training, name, minibatchSize, learningRate, tp)
  }

  private def sequential(validation: CorpusDataset, training: (String) => CorpusDataset, name: String, minibatchSize: Seq[Int], learningRate: Seq[Double], trainer: (NeuralPrefs, IteratorPrefs) => NeuralResult) = {
    for {lr <- learningRate; mbs <- minibatchSize} {
      val prefs = NeuralPrefs(learningRate = lr, epochs = 1, minibatchSize = mbs)
      val allRes: Seq[Seq[NeuralEvaluation]] = Config.cats.map(c => {
        val trainingPrefs: IteratorPrefs = IteratorPrefs(c, training(c), validation)
        val res: NeuralResult = trainer(prefs, trainingPrefs)
        res.evaluations
      })
      printResults(allRes, name)
    }
  }

  private def parallel(validation: CorpusDataset, train: CorpusDataset, name: String, minibatchSize: Seq[Int], learningRate: Seq[Double], trainer: (NeuralPrefs, IteratorPrefs) => NeuralResult, parallelism: Int) = {
    // Force pre-generation of document vectors before entering Spark to avoid passing W2V references between executors
    Log.v("Broadcasting dataset ...")
    Log.v("Starting distributed training ...")
    val cats = Config.cats.par
    cats.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(Config.parallelism))
    // TODO: SPARK THIS UP, BUT DON'T FORGET THE W2V LOADER!
    for {lr <- learningRate; mbs <- minibatchSize} {
      val allRes: Seq[Seq[NeuralEvaluation]] = cats.map(c => {
        val prefs: NeuralPrefs = NeuralPrefs(learningRate = lr, epochs = 1, minibatchSize = mbs)
        val trainingPrefs: IteratorPrefs = IteratorPrefs(c, train, validation)
        val res: NeuralResult = trainer(prefs, trainingPrefs)
        res.evaluations
      }).seq
      printResults(allRes, name)
    }
  }

  private def timeLeft(totalTrainingSize: Int, currentIteration: Int, batch: Int, label: String, currentEpoch: Int, totalEpoch: Int): Int = {
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

  private def printResults(allRes: Seq[Seq[NeuralEvaluation]], name: String) = {
    val epochs = allRes.head.size
    Log.v("Accumulating results ...")
    val resultFile = s"train/$name.txt"
    Log.toFile("", resultFile)
    Log.toFile("Model: " + name + " - Args: " + Config.getArgs, resultFile)
    for (i <- 0 until epochs) {
      val labelEvals: Seq[NeuralEvaluation] = allRes.map(_ (i)).sortBy(_.label)
      NeuralEvaluation.logLabelStats(labelEvals, resultFile)
      NeuralEvaluation.log(labelEvals, resultFile, Config.cats, i)
    }
  }
}
