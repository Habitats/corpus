package no.habitats.corpus.common.models

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

case class DBPediaAnnotation(articleId: String, mc: Int, entity: Entity) {
  def id = articleId + "_" + entity.id

}

object DBPediaAnnotation {
  implicit val formats = Serialization.formats(NoTypeHints)

  def toStringSerialized(dBPediaAnnotation: DBPediaAnnotation): String = {
    dBPediaAnnotation.articleId + "\t" + dBPediaAnnotation.mc + "\t\t" + Entity.toStringSerialized(dBPediaAnnotation.entity)
  }

  def fromStringSerialized(string: String): DBPediaAnnotation = {
    val entityStart = string.indexOf("\t\t")
    val dbFields = string.substring(0, entityStart)
    val dbSplitIndex: Int = dbFields.indexOf("\t")
    val articleId: String = dbFields.substring(0, dbSplitIndex)
    val mc: Int = dbFields.substring(dbSplitIndex + 1, dbFields.length).toInt
    val entity: Entity = Entity.fromStringSerialized(string.substring(entityStart +2 , string.length))
    DBPediaAnnotation(articleId, mc, entity)
  }

  def toSingleJson(dBPediaAnnotation: DBPediaAnnotation): String = {
    write(dBPediaAnnotation)
  }

  def fromSingleJson(string: String): DBPediaAnnotation = {
    read[DBPediaAnnotation](string)
  }
}
