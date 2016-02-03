package no.habitats.corpus

import java.io.{File, PrintWriter}

import com.nytlabs.corpus.{NYTCorpusDocument, NYTCorpusDocumentParser}
import no.habitats.corpus.models.{Annotation, Annotations, Article, ArticleWrapper}
import no.habitats.corpus.features.FreeBase

import scala.collection.mutable.ListBuffer
import scala.io.{Codec, Source}
import scala.util.Try

object Corpus {

  def walk(root: File, filter: String = "", onlyRelevant: Set[String] = Set()): Seq[File] = {
    def filterFile(f: File): Boolean = {
      val name = f.getName
      // if its a directory, keep it
      if (f.isDirectory) true
      // if its in the main filter, investigate further
      else if (name.endsWith(filter)) {
        // if relevant set contains something
        if (onlyRelevant.nonEmpty) {
          val id = name.split("\\.").head
          if (onlyRelevant.contains(id)) {
            IO.copy(f)
            return onlyRelevant.contains(id)
          } else return false
        }
        // if the relevant set contains this ID
        true
      }
      else false
    }
    if (root.isDirectory) root.listFiles.flatMap(f => walk(f, filter, onlyRelevant)).filter(filterFile).distinct
    else Seq(root).distinct
  }

  lazy val nytWalk    = walk(new File(Config.testPath + Config.data), ".xml")
  lazy val googleWalk = walk(new File(Config.testPath + Config.data), ".txt")

  lazy val relevantArticleIds: Set[String] = Try(Config.dataFile("google_annotations/relevant_article_ids.txt").getLines().toSet).getOrElse(Set())

  def articles(files: Seq[File] = nytWalk, count: Int = Int.MaxValue, from: Int = 0): Seq[Article] = {
    val p = new NYTCorpusDocumentParser
    Log.v("Loading articles ...")
    val articles = files
      .slice(from, from + count)
      .map(f => p.parseNYTCorpusDocumentFromFile(f, false))
      .filter(_ != null)
      .map(f => ArticleWrapper(f).toArticle)
    Log.v("Generated " + articles.size + " articles")
    articles.seq
  }

  def rawArticles(files: Seq[File] = nytWalk): Seq[NYTCorpusDocument] = {
    val p = new NYTCorpusDocumentParser
    files.map(f => p.parseNYTCorpusDocumentFromFile(f, false))
  }

  def toAnnotation(line: String, id: String): Annotation = {
    // the file's a little funky, and they use tabs and spaces as different delimiters
    val arr = line.split("\\t")
    Annotation(articleId = id, index = arr(0).toInt, mc = arr(2).toInt, phrase = arr(3), offset = arr(4).toInt, fb = arr(6), wd = FreeBase.fbToWikiMapping.getOrElse(arr(6), "NONE"))
  }

  def toAnnotations(file: File): Seq[Annotations] = {
    val reader = Source.fromFile(file)(Codec.ISO8859).bufferedReader()
    val articles = ListBuffer[Seq[String]]()
    var line = reader.readLine
    while (line != null) {
      val lines = ListBuffer[String]()
      if (line.length > 0) {
        while (line != null && line.trim.length > 0) {
          lines += line
          line = reader.readLine()
        }
        articles += lines
        line = reader.readLine()
      } else {
        line = reader.readLine()
      }
    }
    articles.map(lines => {
      val firstLineArr = lines.head.split("\\t")
      val id = firstLineArr(0)
      val annotations = lines.takeRight(lines.size - 1).map(l => toAnnotation(l, id))
      Annotations(articleId = id, annotations = annotations)
    })
  }

  def cacheAnnotationIds(annotations: Set[String]) = {
    val f = new File("data/google_annotations/relevant_article_ids.txt")
    if (!f.exists) {
      Log.v("Caching annotation id's ...")
      f.createNewFile
      val writer = new PrintWriter(f)
      annotations.foreach(id => writer.println(id))
      writer.close()
      Log.v("Caching annotation complete!")
    }
  }

  def annotations(files: Seq[File] = googleWalk, count: Int = Int.MaxValue, from: Int = 0): Seq[Annotations] = {
    val annotations = files.flatMap(toAnnotations).slice(from, from + count)
    Log.v("Generated " + annotations.size + " annotations")
    cacheAnnotationIds(annotations.map(_.articleId).toSet)
    annotations
  }

  def annotatedArticles(articles: Seq[Article], annotations: Seq[Annotations]): Seq[Article] = {
    val all = articles.map(a => (a.id, a)).toMap
    annotations.filter(ann => all.get(ann.articleId).nonEmpty)
      .map(annotationWrapper => {
        val article = all(annotationWrapper.articleId)
        val annotationMap = annotationWrapper.annotations.map(ann => (ann.id, ann)).toMap
        article.copy(ann = annotationMap)
      })
  }
}





