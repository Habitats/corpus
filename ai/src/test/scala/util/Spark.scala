package util

import no.habitats.corpus.common.CorpusContext

trait Spark {
  lazy val sc = CorpusContext.sc
}
