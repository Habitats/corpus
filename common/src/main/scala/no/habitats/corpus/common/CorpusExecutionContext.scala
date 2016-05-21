package no.habitats.corpus.common

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

/**
  * Created by mail on 21.05.2016.
  */
object CorpusExecutionContext {
  implicit val executionContext = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(10)
    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable) {}
  }
}
