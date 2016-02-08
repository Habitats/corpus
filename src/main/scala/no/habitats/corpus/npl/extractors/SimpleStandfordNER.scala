package no.habitats.corpus.npl.extractors

import edu.stanford.nlp.simple.Document
import no.habitats.corpus.npl.models.Sentence

import scala.collection.JavaConverters._

object SimpleStandfordNER extends App with Extractor {

  def extract(text: String): Seq[Sentence] = {
    val doc = new Document(text)
    val sentences = for {
      sentence <- doc.sentences().asScala
      words = sentence.words().asScala
      lemma = sentence.lemmas().asScala
      parsed = sentence.posTags().asScala
      ners = sentence.nerTags().asScala
    } yield Sentence(words, lemma, parsed, ners)

    sentences
  }
}
