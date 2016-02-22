package no.habitats.corpus.models

import java.io.File

import no.habitats.corpus.Config
import no.habitats.corpus.models.Annotation.NONE
import no.habitats.corpus.npl.WikiData

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Codec, Source}

case class Annotation(articleId: String,
                      phrase: String, // phrase
                      mc: Int, // mention count
                      offset: Int = -1,
                      fb: String = NONE, // Freebase ID
                      wd: String = NONE, // WikiData ID
                      db: String = NONE,
                      tfIdf: Double = -1 // term frequency, inverse document frequency
                     ) {

  lazy val id: String = {
    if (fb == NONE && wd == NONE) phrase
    else if (fb != NONE) fb
    else if (WikiData.wdToFb.contains(wd)) WikiData.wdToFb(wd)
    else wd
  }

  // Create annotation from WikiDAta ID
  def fromWd(phrase: String): Annotation = copy(phrase = wd + " - " + phrase)

  override def toString: String = f"id: $id%20s > fb: $fb%10s > wb: $wd%10s > offset: $offset%5d > phrase: $phrase%50s > mc: $mc%3d > TF-IDF: $tfIdf%.10f"
}

object Annotation {
  val NONE = "NONE"

  def fromWikidata(articleId: String, wd: Entity): Annotation = {
    new Annotation(articleId = articleId, phrase = wd.name, mc = 1, wd = wd.id)
  }

  // from POS name
  def fromName(articleId: String, index: Int, name: String, count: Int, kind: String): Annotation = {
    new Annotation(articleId = articleId, phrase = name, mc = count)
  }

  // google annotations raw lines format
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
          wd = WikiData.fbToWd.getOrElse(arr(6), NONE)
        )
      })
      (articleId, ann)
    }).toMap
  }

  def fromDbpedia(dbpedia: DBPediaAnnotation): Annotation = {
    val wd = WikiData.dbToWd.getOrElse(dbpedia.entity.id, NONE)
    val fb = WikiData.wdToFb.getOrElse(wd, NONE)
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
}

