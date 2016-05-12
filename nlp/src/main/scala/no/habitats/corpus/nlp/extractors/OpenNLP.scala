package no.habitats.corpus.nlp.extractors

import java.io.FileInputStream

import no.habitats.corpus.common.Config
import no.habitats.corpus.nlp.models.Sentence
import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.stemmer.PorterStemmer
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

object OpenNLP extends Extractor {

  private lazy val sentenceDetector = {
    val model = new SentenceModel(new FileInputStream(Config.dataPath + "opennlp/en-sent.bin"))
    new SentenceDetectorME(model)
  }

  private lazy val tokenizer = {
    val model = new TokenizerModel(new FileInputStream(Config.dataPath + "opennlp/en-token.bin"))
    new TokenizerME(model)
  }

  private def nameFinderME(model: String): NameFinderME = {
    val m = new TokenNameFinderModel(new FileInputStream(Config.dataPath + model))
    new NameFinderME(m)
  }

  private lazy val posTagger = {
    val model = new POSModel(new FileInputStream(Config.dataPath + "opennlp/en-pos-maxent.bin"))
    new POSTaggerME(model)
  }

  private lazy val persons   = nameFinderME("opennlp/en-ner-person.bin")
  private lazy val orgs      = nameFinderME("opennlp/en-ner-organization.bin")
  private lazy val locations = nameFinderME("opennlp/en-ner-location.bin")
  private lazy val money     = nameFinderME("opennlp/en-ner-money.bin")
  private lazy val date      = nameFinderME("opennlp/en-ner-time.bin")

  def pair(model: NameFinderME, tokens: Array[String]): Array[(String, String)] = model.find(tokens).map(n => (n.getType, tokens.slice(n.getStart, n.getEnd).map(_.trim).mkString(" ")))

  def sentenceDetection(text: String): Array[String] = sentenceDetector.sentDetect(text)

  def tokenize(text: String): Array[String] = tokenizer.tokenize(text)

  def nameFinder(text: String): Array[(String, String)] = {
    val models = Seq(persons, locations, money, orgs)
    val sentences = sentenceDetection(text)
    val names = sentences.map(s => tokenize(s)).flatMap(tokens => models.flatMap(m => pair(m, tokens)))
    models.foreach(_.clearAdaptiveData())
    names
  }

  def nameFinderCounts(text: String): Map[String, (Int, String)] = {
    nameFinder(text).groupBy(_._2).map { case (name, list) => (name, (list.length, list(0)._1)) }
  }

  def nameFinderOrdered(text: String): Map[String, Seq[String]] = {
    nameFinder(text).groupBy(_._1).map(w => (w._1, w._2.map(_._2).distinct.toSeq.sorted))
  }

  lazy val stemmer = new PorterStemmer()

  // Tags: https://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html
  def nouns(text: String) = {
    sentenceDetection(text)
      .flatMap(tokenize).map(tokens => posTagger.tag(tokens))
      .map(_.split("/"))
      .filter(w => w(1) == "NN" || w(1) == "NNS" || w(1) == "NNP" || w(1) == "NNPS")
      .map(w => if (w(1) == "NN") w(0) else stemmer.stem(w(0)))
  }

  def namedEntities(tokens: Array[String]) = {
    val p = persons.find(tokens)
    val o = orgs.find(tokens)
    val l = locations.find(tokens)
    val m = money.find(tokens)
    val t = date.find(tokens)
    var pi, oi, li, mi, ti = 0
    val ner = tokens.indices.map { i =>
      val pp = if (p.length > pi && p(pi).getStart == i) {
        pi += 1
        Some(p(pi - 1))
      } else None
      val oo = if (o.length > oi && o(oi).getStart == i) {
        oi += 1
        Some(o(oi - 1))
      } else None
      val ll = if (l.length > li && l(li).getStart == i) {
        li += 1
        Some(l(li - 1))
      } else None
      val mm = if (m.length > mi && m(mi).getStart == i) {
        mi += 1
        Some(m(mi - 1))
      } else None
      val tt = if (t.length > ti && t(ti).getStart == i) {
        ti += 1
        Some(t(ti - 1))
      } else None
      val all = Set(pp, oo, ll, mm)
      val flat = all.flatten
      val mostProbable = if (flat.nonEmpty) flat.maxBy(_.getProb).getType else "0"
      mostProbable
    }

    ner
  }

  override def extract(text: String): Seq[Sentence] = {
    val sentences = sentenceDetection(text)
    sentences.map(sentence => {
      val tokens = tokenize(sentence)
      val lemma = tokens.map(stemmer.stem)
      val pos = posTagger.tag(tokens)
      val ner = namedEntities(tokens)
      Sentence(tokens, lemma, pos, ner)
    })
  }
}
