package no.habitats.corpus.nlp.extractors

import no.habitats.corpus.nlp.models.Sentence

/**
  * Created by mail on 04.02.2016.
  */
trait Extractor {
  def extract(text: String): Seq[Sentence]
  def tokenize(text: String): Array[String]
}
