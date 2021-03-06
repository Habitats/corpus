package no.habitats.corpus

import java.io.File

import com.nytlabs.corpus.NYTCorpusDocumentParser
import no.habitats.corpus.common._
import no.habitats.corpus.common.models._
import no.habitats.corpus.nlp.extractors.Simple

object Corpus {

  lazy val rawNYTParser = new NYTCorpusDocumentParser

  lazy val googleAnnotations: Map[String, Seq[Annotation]] = {
    val annotations = AnnotationUtils.fromGoogle()
    Log.v("Generated " + annotations.size + " annotations")
    Log.saveToList(annotations.keySet, Config.dataPath + "/nyt/nyt_with_google-annotations.txt")
    annotations
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

  def toArticle(nyt: NYTCorpusDocument): Article = ArticleUtils.fromNYT(nyt)

  def toIPTC(article: Article): Article = article.addIptc(Config.broadMatch)

  def fromBody(articleId: String, body: String): Map[String, Annotation] = {
    Simple.tokenize(body.toLowerCase)
      .zipWithIndex
      .map(w => Annotation(articleId = articleId, phrase = w._1, mc = 1, offset = w._2))
      .groupBy(_.id).map(group => (group._1, group._2.head.copy(mc = group._2.map(_.mc).sum, offset = group._2.map(_.offset).min)))
  }

  def toTraditional(article: Article): Article = {
    article.copy(ann = fromBody(article.id, article.body))
  }

  def toGoogleAnnotated(a: Article): Article = {
    googleAnnotations.get(a.id) match {
      case Some(ann) => a.copy(ann = a.ann ++ ann.map(k => k.id -> k).toMap)
      case None => a
    }
  }

}

object ArticleUtils {

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




