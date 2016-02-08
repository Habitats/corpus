package no.habitats.corpus

import java.io.{File, PrintWriter}

import com.nytlabs.corpus.{NYTCorpusDocument, NYTCorpusDocumentParser}
import no.habitats.corpus.models.{Annotation, Article}

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

  lazy val relevantArticleIds: Set[String] = Try(Config.dataFile("google_annotations/relevant_article_ids.txt").getLines().toSet).getOrElse(Set())

  def articles(files: Seq[File] = nytWalk, count: Int = Int.MaxValue): Seq[Article] = {
    val p = new NYTCorpusDocumentParser
    Log.v("Loading articles ...")
    val articles = files
      .map(f => p.parseNYTCorpusDocumentFromFile(f, false))
      .filter(_ != null)
      .map(Article(_))
    Log.v("Generated " + articles.size + " articles")
    articles.seq
  }

  def rawArticles(files: Seq[File] = nytWalk): Seq[NYTCorpusDocument] = {
    val p = new NYTCorpusDocumentParser
    files.map(f => p.parseNYTCorpusDocumentFromFile(f, false))
  }

  def cacheAnnotationIds(annotations: Set[String]) = {
    val f = new File(Config.dataRoot + "google-annotations/relevant_article_ids.txt")
    if (!f.exists) {
      Log.v("Caching annotation id's ...")
      f.createNewFile
      val writer = new PrintWriter(f)
      annotations.foreach(id => writer.println(id))
      writer.close()
      Log.v("Caching annotation complete!")
    }
  }

  def annotations(): Map[String, Seq[Annotation]] = {
    val annotations = Annotation.fromGoogle()
    Log.v("Generated " + annotations.size + " annotations")
    cacheAnnotationIds(annotations.keySet)
    annotations
  }

  def annotatedArticles(articles: Seq[Article] = Corpus.articles(), annotations: Map[String, Seq[Annotation]] = Corpus.annotations()): Seq[Article] = {
    articles.map(a => {
      val updatedAnn = annotations(a.id).map(k => k.id -> k).toMap
      a.copy(ann = updatedAnn)
    })
  }
}





