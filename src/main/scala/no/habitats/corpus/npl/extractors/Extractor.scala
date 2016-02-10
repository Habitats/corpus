package no.habitats.corpus.npl.extractors

import no.habitats.corpus.npl.models.Sentence

/**
  * Created by mail on 04.02.2016.
  */
trait Extractor {
  def extract(text: String): Seq[Sentence]
}
