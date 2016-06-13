package no.habitats.corpus.npl.extractors

import java.io.StringReader

import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.process.PTBTokenizer.PTBTokenizerFactory
import edu.stanford.nlp.process.Tokenizer
import edu.stanford.nlp.simple.Document
import no.habitats.corpus.nlp.extractors.Extractor
import no.habitats.corpus.nlp.models.Sentence

import scala.collection.JavaConverters._
import scala.collection.mutable

object SimpleStandfordNER extends App with Extractor {

  lazy val tokenizerFactory = PTBTokenizerFactory.newPTBTokenizerFactory(false, false)

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

  def tokenize(text: String): Array[String] = {
    val reader: StringReader = new StringReader(text)
    val tokenizer: Tokenizer[CoreLabel] = tokenizerFactory.getTokenizer(reader)
    val tokens: mutable.Buffer[CoreLabel] = tokenizer.tokenize().asScala
    tokens.map(_.value).toArray
  }
}
