package no.habitats.corpus.models

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

object DBPediaAnnotation {
  implicit val formats = Serialization.formats(NoTypeHints)

  def toSingleJson(dBPediaAnnotation: DBPediaAnnotation): String = {
    write(dBPediaAnnotation)
  }

  def fromSingleJson(string: String): DBPediaAnnotation = {
    read[DBPediaAnnotation](string)
  }
}

case class DBPediaAnnotation(articleId: String, mc: Int, entity: Entity) {
  def id = articleId + "_" + entity.id
}
