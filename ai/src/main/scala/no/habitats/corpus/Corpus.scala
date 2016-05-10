package no.habitats.corpus

import java.io.File

import com.nytlabs.corpus.NYTCorpusDocumentParser
import no.habitats.corpus.common._
import no.habitats.corpus.common.models._

object Corpus {

  lazy val rawNYTParser = new NYTCorpusDocumentParser

  lazy val googleAnnotations: Map[String, Seq[Annotation]] = {
    val annotations = AnnotationUtils.fromGoogle()
    Log.v("Generated " + annotations.size + " annotations")
    Log.toFile(annotations.keySet, "/nyt/nyt_with_google-annotations.txt")
    annotations
  }

  lazy val dbpediaAnnotations       : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpedia)
  lazy val dbpediaAnnotationsMini25 : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini25)
  lazy val dbpediaAnnotationsMini50 : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini50)
  lazy val dbpediaAnnotationsMini75 : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini75)
  lazy val dbpediaAnnotationsMini100: Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini100)

  def fetchDbpediaAnnotations(dbpedia: String): Map[String, Seq[Annotation]] = {
    CorpusContext.sc.textFile(dbpedia)
      .map(DBPediaAnnotation.fromSingleJson)
      .map(AnnotationUtils.fromDbpedia)
//      .filter(an => an.fb != Config.NONE && W2VLoader.contains(an.fb))
      .groupBy(_.articleId)
      .map { case (k, v) => (k, v.toSeq) }
      .collect.toMap
  }

  lazy val dbpediaAnnotationsWithTypes: Map[String, Seq[Annotation]] = {
    CorpusContext.sc.textFile(Config.dbpedia)
      .map(DBPediaAnnotation.fromSingleJson)
      .flatMap(ann => Seq(AnnotationUtils.fromDbpedia(ann)) ++ AnnotationUtils.fromDBpediaType(ann))
      .groupBy(_.articleId)
      .filter(_._2.nonEmpty)
      .map { case (k, v) => (k, v.toSeq.filter(ann => W2VLoader.contains(ann.fb))) }
      .collect.toMap
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

  def toDBPediaAnnotated(a: Article, dbpediaAnnotations: Map[String, Seq[Annotation]]): Article = {
    dbpediaAnnotations.get(a.id) match {
      case Some(ann) => a.copy(ann = a.ann ++ ann.map(a => (a.id, a)).toMap)
      case None => Log.v("NO DBPEDIA: " + a.id); a
    }
  }

  def toDBPediaAnnotatedWithTypes(a: Article, dbpediaAnnotations: Map[String, Seq[Annotation]]): Article = {
    dbpediaAnnotationsWithTypes.get(a.id) match {
      case Some(ann) => a.copy(ann = a.ann ++ ann.map(a => (a.id, a)).filter(a => a._2.fb != Config.NONE && W2VLoader.contains(a._1)).toMap)
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




