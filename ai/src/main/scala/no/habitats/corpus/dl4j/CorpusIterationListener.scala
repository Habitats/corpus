package no.habitats.corpus.dl4j

import no.habitats.corpus.common.Log
import org.deeplearning4j.nn.api.Model
import org.deeplearning4j.optimize.api.IterationListener

case class CorpusIterationListener extends IterationListener {
  private var printIterations: Int     = 1
  private var inv            : Boolean = false
  private var iterCount      : Long    = 0

  def this(printIterations: Int) {
    this()
    this.printIterations = printIterations
  }

  def invoked: Boolean = inv

  def invoke() {
    this.inv = true
  }

  def iterationDone(model: Model, iteration: Int) {
    if (printIterations <= 0) printIterations = 1
    if (iterCount % printIterations == 0) {
      invoke
      val result: Double = model.score
      Log.r("Score at iteration " + iterCount + " is " + result, "iterations.txt")
    }
    iterCount += 1
  }
}
