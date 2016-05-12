package no.habitats.corpus.nlp.models

/**
  * Created by mail on 04.02.2016.
  */
case class Sentence(words: Seq[Word]) {
  override def toString = {
    Seq(
      words.map(w => f"${w.raw}%12s").mkString(" "),
      words.map(w => f"${w.lemma}%12s").mkString(" "),
      words.map(w => f"${w.tag}%12s").mkString(" "),
      words.map(w => f"${w.ner}%12s").mkString(" ")
    ).mkString("", "\n", "\n")
  }
}

object Sentence {
  def apply(words: Seq[String], lemmas: Seq[String], pos: Seq[String], ner: Seq[String]) = {
    new Sentence(for (i <- words.indices) yield Word(words(i), lemmas(i), pos(i), ner(i)))
  }

}
