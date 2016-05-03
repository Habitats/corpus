package no.habitats.corpus

import java.io.File

import com.nytlabs.corpus.NYTCorpusDocumentParser
import no.habitats.corpus.common.models._
import no.habitats.corpus.common.{AnnotationUtils, Config, Log}

object Corpus {

  lazy val rawNYTParser = new NYTCorpusDocumentParser

  lazy val googleAnnotations: Map[String, Seq[Annotation]] = {
    val annotations = AnnotationUtils.fromGoogle()
    Log.v("Generated " + annotations.size + " annotations")
    Log.toFile(annotations.keySet, "/nyt/nyt_with_google-annotations.txt")
    annotations
  }

  lazy val dbpediaAnnotations: Map[String, Seq[Annotation]] = {
    val path = Config.dbpedia
    Log.v(s"Loading $path ...")
    Config.dataFile(path).getLines()
      .map(DBPediaAnnotation.fromSingleJson)
      .map(AnnotationUtils.fromDbpedia)
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

  def toArticle(nyt: NYTCorpusDocument): Article = Article.fromNYT(nyt)

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

object Article {

  import scala.collection.JavaConverters._

  implicit def stringToOption(s: String): Option[String] = Option(s)
  implicit def intToOption(s: Integer): Option[String] = if (s != null) Some(s.toString) else None
  implicit def getEither(s: (String, String)): String = if (s._1 != null) s._1 else s._2

  def allDescriptors(a: NYTCorpusDocument): Set[String] = {
    val tax = a.getTaxonomicClassifiers.asScala.map(_.split("/").last).toSet
    val desc = (a.getDescriptors.asScala ++ a.getOnlineDescriptors.asScala ++ a.getGeneralOnlineDescriptors.asScala).toSet
    desc.union(tax).map(_.toLowerCase)
  }

  def fromNYT(a: NYTCorpusDocument): Article = {
    new Article(
      id = a.getGuid.toString,
      hl = (a.getHeadline, a.getOnlineHeadline),
      body = a.getBody,
      wc = if (a.getWordCount != null) a.getWordCount else if (a.getBody != null) a.getBody.split("\\s+").length else 0,
      desc = allDescriptors(a),
      date = a.getPublicationDate.getTime.toString,
      url = a.getUrl.toString
    )
  }
}




