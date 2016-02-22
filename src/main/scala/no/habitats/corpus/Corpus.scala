package no.habitats.corpus

import java.io.File

import com.nytlabs.corpus.{NYTCorpusDocument, NYTCorpusDocumentParser}
import no.habitats.corpus.models.{Annotation, Article, DBPediaAnnotation}

import scala.util.Try

object Corpus {

  lazy val relevantArticleIds: Set[String] = Try(Config.dataFile("google_annotations/relevant_article_ids.txt").getLines().toSet).getOrElse(Set())
  lazy val rawNYTParser = new NYTCorpusDocumentParser

  lazy val annotations: Map[String, Seq[Annotation]] = {
    val annotations = Annotation.fromGoogle()
    Log.v("Generated " + annotations.size + " annotations")
    Log.toFile(annotations.keySet, "/nyt/nyt_with_google-annotations.txt")
    annotations
  }

  lazy val dbpediaAnnotations: Map[String, Seq[Annotation]] = {
    Config.dataFile("nyt/" + Config.dbpedia).getLines()
      .map(DBPediaAnnotation.fromSingleJson)
      .map(Annotation.fromDbpedia)
      .toList.groupBy(_.articleId)
  }

  // all at once
  def rawArticles(path: String = Config.dataPath + "/nyt/", count: Int = Config.count): Seq[NYTCorpusDocument] = {
    IO.walk(path, count, ".xml").map(f => rawNYTParser.parseNYTCorpusDocumentFromFile(f, false))
  }

  def articles(path: String = Config.dataPath + "/nyt/", count: Int = Config.count): Seq[Article] = {
    Log.v(f"Loading $count articles ...")
    IO.walk(path, count, filter = ".xml")
      .map(toNYT)
      .map(toArticle)
      .map(toIPTC)
      .map(toDBPedia)
  }

  def annotatedArticles(articles: Seq[Article] = Corpus.articles()): Seq[Article] = {
    articles.map(toAnnotated)
  }

  // transformations
  def toNYT(file: File): NYTCorpusDocument = rawNYTParser.parseNYTCorpusDocumentFromFile(file, false)
  def toArticle(nyt: NYTCorpusDocument): Article = Article(nyt)
  def toAnnotated(a: Article): Article = {
    annotations.get(a.id) match {
      case Some(v) => a.copy(ann = v.map(k => k.id -> k).toMap)
      case None => a
    }
  }
  def toIPTC(article: Article): Article = article.addIptc(Config.broadMatch)
  def toDBPedia(article: Article): Article = {
    val ann = dbpediaAnnotations.get(article.id) match {
      case Some(ann) => ann.map(a => (a.id, a)).toMap
      case None => /**Log.v("NO DBPEDIA: " + article.id);*/ Map[String, Annotation]()
    }
    article.copy(ann = ann)
  }
}





