package no.habitats.corpus.common.models

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

case class DBPediaAnnotation(articleId: String, mc: Int, entity: Entity) {
  def id = articleId + "_" + entity.id
}

object DBPediaAnnotation {
  implicit val formats = Serialization.formats(NoTypeHints)

  def serialize(dBPediaAnnotation: DBPediaAnnotation): String = {
    dBPediaAnnotation.articleId + "\t" + dBPediaAnnotation.mc + "\t\t" + Entity.serialize(dBPediaAnnotation.entity)
  }

  def deserialize(string: String): DBPediaAnnotation = {
    val entityStart = string.indexOf("\t\t")
    val dbFields = string.substring(0, entityStart)
    val dbSplitIndex: Int = dbFields.indexOf("\t")
    val articleId: String = dbFields.substring(0, dbSplitIndex)
    val mc: Int = dbFields.substring(dbSplitIndex + 1, dbFields.length).toInt
    val entity: Entity = Entity.deserialize(string.substring(entityStart +2 , string.length))
    DBPediaAnnotation(articleId, mc, entity)
  }

  @Deprecated
  def toSingleJson(dBPediaAnnotation: DBPediaAnnotation): String = {
    write(dBPediaAnnotation)
  }

  @Deprecated
  def fromSingleJson(string: String): DBPediaAnnotation = {
    read[DBPediaAnnotation](string)
  }
}
