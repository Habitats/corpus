package no.habitats.corpus.nlp.extractors

import no.habitats.corpus.common.Config
import no.habitats.corpus.nlp.models.Sentence
import opennlp.tools.stemmer.PorterStemmer

/**
  * Created by mail on 12.06.2016.
  */
object Simple extends Extractor {

  val ordinary: Set[Char] = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet ++ Set('$', '€', '¥')
  def isOrdinary(s: String) = s.forall(ordinary.contains)
  val stopwords = Config.dataFile(Config.dataPath + "stopwords.txt").getLines().toSet

  override def tokenize(test: String): Array[String] = {
    val stemmer = new PorterStemmer
    test.toLowerCase
      .replaceAll("[^a-z\\$¥€\\s+]", "")
      .split("\\s+")
      .map(stemmer.stem)
    //      .filter(w => !stopwords.contains(w))
    //      .filter(isOrdinary)
  }

  override def extract(text: String): Seq[Sentence] = ???
}
