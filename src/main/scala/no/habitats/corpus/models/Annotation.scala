package no.habitats.corpus.models

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import no.habitats.corpus.Config
import no.habitats.corpus.npl.WikiData

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Codec, Source}

case class Annotation(articleId: String,
                      index: Int, // index
                      phrase: String, // phrase
                      mc: Int, // mention count
                      offset: Int = -1,
                      fb: String = "NONE", // Freebase ID
                      wd: String = "NONE", // WikiData ID
                      tfIdf: Double = -1 // term frequency, inverse document frequency
                     ) {

  lazy val id: String = {
    if (fb == "NONE" && wd == "NONE") phrase
    else if (fb != "NONE") fb
    else if (WikiData.wikiToFbMapping.contains(wd)) WikiData.wikiToFbMapping(wd)
    else wd
  }

  // Create annotation from WikiDAta ID
  def fromWd(index: Int, phrase: String): Annotation = copy(index = index, phrase = wd + " - " + phrase)

  override def toString: String = f"id: $id%20s > fb: $fb%10s > wb: $wd%10s > offset: $offset%5d > phrase: $phrase%50s > mc: $mc%3d > TF-IDF: $tfIdf%.10f"
}

object Annotation {

  val nextId = new AtomicInteger((System.currentTimeMillis() / 100000).toInt)

  // from google annotations
  def apply(line: String, id: String): Annotation = {
    // the file's a little funky, and they use tabs and spaces as different delimiters
    val arr = line.split("\\t")
    new Annotation(
      articleId = id,
      index = arr(0).toInt,
      phrase = arr(3),
      mc = arr(2).toInt,
      offset = arr(4).toInt,
      fb = arr(6),
      wd = WikiData.fbToWikiMapping.getOrElse(arr(6), "NONE")
    )
  }

  def fromWikidata(articleId: String, wd: Entity): Annotation = {
    new Annotation(articleId = articleId, index = nextId.incrementAndGet(), phrase = wd.name, mc = 1, wd = wd.id)
  }

  // from POS name
  def fromName(articleId: String, index: Int, name: String, count: Int, kind: String): Annotation = {
    new Annotation(articleId = articleId, index = index, phrase = name, mc = count)
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
      val ann = lines.slice(1, lines.length).map(l => Annotation(l, articleId))
      (articleId, ann)
    }).toMap
  }
}

