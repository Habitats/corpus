package no.habitats.corpus.features

import java.io.FileInputStream

import no.habitats.corpus.Config
import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.stemmer.PorterStemmer
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

object POS extends App {
  private lazy val sentenceDetector = {
    val model = new SentenceModel(new FileInputStream(Config.dataRoot + "pos/en-sent.bin"))
    new SentenceDetectorME(model)
  }

  private lazy val tokenizer = {
    val model = new TokenizerModel(new FileInputStream(Config.dataRoot + "pos/en-token.bin"))
    new TokenizerME(model)
  }

  private def nameFinderME(model: String): NameFinderME = {
    val m = new TokenNameFinderModel(new FileInputStream(Config.dataRoot + model))
    new NameFinderME(m)
  }

  private lazy val posTagger = {
    val model = new POSModel(new FileInputStream(Config.dataRoot + "pos/en-pos-maxent.bin"))
    new POSTaggerME(model)
  }

  lazy val test = Config.dataFile("pos/article.txt").getLines().mkString(" ")

  private lazy val persons   = nameFinderME("pos/en-ner-person.bin")
  private lazy val orgs      = nameFinderME("pos/en-ner-organization.bin")
  private lazy val locations = nameFinderME("pos/en-ner-location.bin")
  private lazy val money     = nameFinderME("pos/en-ner-money.bin")

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
    nameFinder(text).groupBy(_._2).map{case (name, list) => (name, (list.length, list(0)._1)) }
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
}
