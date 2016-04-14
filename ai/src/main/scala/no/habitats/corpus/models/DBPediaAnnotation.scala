package no.habitats.corpus.models

import no.habitats.corpus.npl.Spotlight
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

case class DBPediaAnnotation(articleId: String, mc: Int, entity: Entity) {
  def id = articleId + "_" + entity.id
}

object DBPediaAnnotation {
  implicit val formats = Serialization.formats(NoTypeHints)

  def toSingleJson(dBPediaAnnotation: DBPediaAnnotation): String = {
    write(dBPediaAnnotation)
  }

  def fromSingleJson(string: String): DBPediaAnnotation = {
    read[DBPediaAnnotation](string)
  }
}
