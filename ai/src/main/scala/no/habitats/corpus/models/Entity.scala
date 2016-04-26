package no.habitats.corpus.models

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

