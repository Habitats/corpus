package util

import no.habitats.corpus.spark.CorpusContext

trait Spark {
  lazy val sc = CorpusContext.sc
}
