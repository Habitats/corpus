package util

import no.habitats.corpus.spark.Context

trait Spark {
  lazy val sc = Context.sc
}
