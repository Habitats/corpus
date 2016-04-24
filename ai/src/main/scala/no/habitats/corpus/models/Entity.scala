package no.habitats.corpus.models

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

case class Entity(
                   id: String,
                   name: String,
                   offset: Int,
                   similarityScore: Double,
                   support: Int,
                   types: Set[String]
                 ) extends JSonable {

  override def toString: String = f"id: $id%40s > offset: $offset%5d > similarity: $similarityScore%.5f > support: $support%5d > types: ${types.mkString(", ")}"
}

