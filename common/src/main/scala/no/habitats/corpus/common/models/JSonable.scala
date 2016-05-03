package no.habitats.corpus.common.models

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

trait JSonable {
  @transient implicit val formats = Serialization.formats(NoTypeHints)
  def toJson = write(this)
}
