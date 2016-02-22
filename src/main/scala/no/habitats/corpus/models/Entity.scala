package no.habitats.corpus.models

case class Entity(
                   id: String,
                   name: String,
                   offset: Int,
                   similarityScore: Double,
                   support: Int,
                   types: Set[String]
                 )
