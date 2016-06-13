package no.habitats.corpus.common

import java.io.File

import no.habitats.corpus.common.models.{Annotation, Article, DBPediaAnnotation, Entity}

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Codec, Source}

object AnnotationUtils {
  val NONE = Config.NONE
  lazy val dw = WikiData.dbToWd
  lazy val wf = WikiData.wdToFb
  lazy val fw = WikiData.fbToWd
  lazy val wd = WikiData.wdToDb

  def fromWikidata(articleId: String, wd: Entity): Annotation = {
    new Annotation(articleId = articleId, phrase = wd.name, mc = 1, wd = wd.id)
  }

  // from POS name
  def fromName(articleId: String, index: Int, name: String, count: Int, kind: String): Annotation = {
    new Annotation(articleId = articleId, phrase = name, mc = count)
  }


  /** Google annotations raw lines format */
  def fromGoogle(file: File = new File(Config.dataPath + "google-annotations/nyt-ann-all.txt")): Map[String, Seq[Annotation]] = {
    val source: BufferedSource = Source.fromFile(file)(Codec.ISO8859)
    val reader = source.bufferedReader()
    val chunks = ListBuffer[Seq[String]]()
    var line = reader.readLine
    while (line != null) {
      val lines = ListBuffer[String]()
      if (line.length > 0) {
        while (line != null && line.trim.length > 0) {
          lines += line
          line = reader.readLine()
        }
        chunks += lines
        line = reader.readLine()
      } else {
        line = reader.readLine()
      }
    }
    chunks.map(lines => {
      val first = lines.head.split("\\t")
      val articleId = first(0)
      val ann = lines.slice(1, lines.length).map(l => {
        val arr = l.split("\\t")
        new Annotation(
          articleId = articleId,
          phrase = arr(3),
          mc = arr(2).toInt,
          offset = arr(4).toInt,
          fb = arr(6),
          wd = fw.getOrElse(arr(6), NONE)
        )
      })
      (articleId, ann)
    }).toMap
  }

  def fromDbpedia(dbpedia: DBPediaAnnotation): Annotation = {
    val wd = dw.getOrElse(dbpedia.entity.id, NONE)
    val fb = wf.getOrElse(wd, NONE)
    new Annotation(
      articleId = dbpedia.articleId,
      phrase = dbpedia.entity.name,
      mc = dbpedia.mc,
      db = dbpedia.entity.id,
      wd = wd,
      fb = fb,
      offset = dbpedia.entity.offset
    )
  }

  def fromDBpediaType(dbpedia: DBPediaAnnotation): Set[Annotation] = {
    dbpedia.entity.types.map(wdType => {
      val fb = wf.getOrElse(wdType, NONE)
      val db = wd.getOrElse(wdType, NONE)
      new Annotation(
        articleId = dbpedia.articleId,
        phrase = "type->" + db,
        mc = dbpedia.mc,
        db = db,
        wd = wdType,
        fb = fb,
        offset = dbpedia.entity.offset
      )
    })
  }
}

