package no.habitats.corpus.models

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

trait JSonable {
  implicit val formats = Serialization.formats(NoTypeHints)
  def toJson = write(this)
}
