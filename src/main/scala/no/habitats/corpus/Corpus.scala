package no.habitats.corpus

import java.io.File

import com.nytlabs.corpus.{NYTCorpusDocument, NYTCorpusDocumentParser}
import no.habitats.corpus.models.{Annotation, Article}

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

  // all at once
  def rawArticles(path: String = Config.dataPath + "/nyt/complete/", count: Int = Config.count): Seq[NYTCorpusDocument] = {
    IO.walk(path, count, ".xml").map(f => rawNYTParser.parseNYTCorpusDocumentFromFile(f, false))
  }

  def articles(path: String = Config.dataPath + "/nyt/complete/", count: Int = Config.count): Seq[Article] = {
    val p = new NYTCorpusDocumentParser
    Log.v(f"Loading $count articles ...")
    val articles = for {
      f <- IO.walk(path, count, ".xml").take(count)
      doc = p.parseNYTCorpusDocumentFromFile(f, false) if doc != null
    } yield Article(doc)
    Log.v("Generated " + articles.size + " articles")
    articles
  }

  def annotatedArticles(articles: Seq[Article] = Corpus.articles()): Seq[Article] = {
    articles.map(toAnnotated)
  }

  // transformations
  def toIPTC(article: Article) = article.addIptc(Config.broadMatch)
  def toNYT(file: File): NYTCorpusDocument = rawNYTParser.parseNYTCorpusDocumentFromFile(file, false)
  def toArticle(nyt: NYTCorpusDocument): Article = Article(nyt)
  def toAnnotated(a: Article): Article = {
    annotations.get(a.id) match {
      case Some(v) => a.copy(ann = v.map(k => k.id -> k).toMap)
      case None => a
    }
  }
}





