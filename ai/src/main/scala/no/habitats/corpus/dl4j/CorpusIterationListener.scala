package no.habitats.corpus.dl4j

import no.habitats.corpus.common.Log
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.bytedeco.javacpp.Pointer
import org.deeplearning4j.nn.api.Model
import org.deeplearning4j.optimize.api.IterationListener

import scala.collection.JavaConverters._

case class CorpusIterationListener() extends IterationListener {
  private var printIterations: Int     = 10
  private var inv            : Boolean = false
  private var iterCount      : Long    = 0
  private val res                      = new CircularFifoQueue[Double](100)

  def this(printIterations: Int) {
    this()
    this.printIterations = printIterations
  }

  def invoked: Boolean = inv

  def invoke() {
    this.inv = true
  }

  def iterationDone(model: Model, iteration: Int) {
    val result: Double = model.score
    res.add(result)
    if (printIterations <= 0) printIterations = 1
    if (iterCount % printIterations == 0) {
      invoke
      Log.r(f"Score at iteration $iterCount is $result%.5f ($average%.5f) [${(Pointer.totalBytes / 10e6).toInt}%4d MB]", "iterations.txt")
    }
    iterCount += 1
  }

  def average: Double = res.asScala.sum / res.size
}
