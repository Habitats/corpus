package no.habitats.corpus

import java.io.File

import com.nytlabs.corpus.{NYTCorpusDocument, NYTCorpusDocumentParser}
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.models.{Annotation, Article, DBPediaAnnotation}

object Corpus {

  lazy val rawNYTParser = new NYTCorpusDocumentParser

  lazy val googleAnnotations: Map[String, Seq[Annotation]] = {
    val annotations = Annotation.fromGoogle()
    Log.v("Generated " + annotations.size + " annotations")
    Log.toFile(annotations.keySet, "/nyt/nyt_with_google-annotations.txt")
    annotations
  }

  lazy val dbpediaAnnotations: Map[String, Seq[Annotation]] = {
    val path = Config.dbpedia
    Log.v(s"Loading $path ...")
    Config.dataFile(path).getLines()
      .map(DBPediaAnnotation.fromSingleJson)
      .map(Annotation.fromDbpedia)
      .toList.groupBy(_.articleId)
  }

  def articlesFromXML(path: String = Config.dataPath + "/nyt/", count: Int = Config.count): Seq[Article] = {
    Log.v(f"Loading ${if (count == Integer.MAX_VALUE) "all" else count} articles ...")
    IO.walk(path, count, filter = ".xml")
      .map(toNYT)
      .map(toArticle)
      .map(toIPTC)
  }

  // transformations
  def toNYT(file: File): NYTCorpusDocument = rawNYTParser.parseNYTCorpusDocumentFromFile(file, false)

  def toArticle(nyt: NYTCorpusDocument): Article = Article(nyt)

  def toIPTC(article: Article): Article = article.addIptc(Config.broadMatch)

  def toGoogleAnnotated(a: Article): Article = {
    googleAnnotations.get(a.id) match {
      case Some(ann) => a.copy(ann = a.ann ++ ann.map(k => k.id -> k).toMap)
      case None => a
    }
  }

  def toDBPediaAnnotated(a: Article): Article = {
    dbpediaAnnotations.get(a.id) match {
      case Some(ann) => a.copy(ann = a.ann ++ ann.map(a => (a.id, a)).toMap)
      case None => Log.v("NO DBPEDIA: " + a.id); a
    }
  }
}





