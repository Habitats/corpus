package no.habitats.corpus.common.models

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

case class Entity(
                   id: String,
                   name: String,
                   offset: Int,
                   similarityScore: Float,
                   support: Int,
                   types: Set[String]
                 ) {

  override def toString: String = f"id: $id%40s > offset: $offset%5d > similarity: $similarityScore%.5f > support: $support%5d > types: ${types.mkString(", ")}"
}

object Entity {
  implicit val formats = Serialization.formats(NoTypeHints)

  def toStringSerialized(entity: Entity): String =
    Array(entity.id, entity.name, entity.offset.toString, entity.similarityScore.toString, entity.support.toString, entity.types.map(_.trim).mkString(",")).mkString("\t")

  def fromStringSerialized(string: String): Entity = {
    val fields = string.split("\t")
    Entity(fields(0), fields(1), fields(2).toInt, fields(3).toFloat, fields(4).toInt, if (fields.length == 6) fields(5).split(",").toSet else Set())
  }

  def toSingleJson(entity: Entity): String = {
    write(entity)
  }

  def fromSingleJson(string: String): Entity = {
    read[Entity](string)
  }
}
