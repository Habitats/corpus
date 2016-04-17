package no.habitats.corpus.npl

import java.io.{File, PrintWriter}

import no.habitats.corpus.common.CorpusContext.sc
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.models.Annotation
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.io.Source

object WikiData {
  implicit val formats = Serialization.formats(NoTypeHints)
  implicit def toJson(url: String): JValue = parse(Source.fromURL(url).mkString)

  val pairsFile = "wikidata/fb_to_wd_all.txt"
  // API roots
  val wmflabs   = "http://wdq.wmflabs.org/api?q="
  val wiki      = "https://www.wikidata.org/w/api.php?format=json&language=en&"

  lazy val instanceOf  = loadPairs("instanceOf.txt")
  lazy val occupations = loadPairs("occupation.txt")
  lazy val genders     = loadPairs("gender.txt")

  lazy val fbToWd: Map[String, String] = loadCachedPairs(Config.dataPath + pairsFile)
  lazy val wdToFb: Map[String, String] = loadCachedPairs(Config.freebaseToWikidata)
  lazy val dbToWd: Map[String, String] = loadCachedPairs(Config.dbpediaToWikidata).map(a => (a._2, a._1))

  /**
    * Load map
    *
    * @param path
    * @return
    */
  def loadCachedPairs(path: String): Map[String, String] = {
    Log.v("Loading " + path + " ...")
    val pairs = sc.textFile(path).map(_.split(" ")).filter(_.length == 2).map(a => (a(0), a(1))).collect.toMap
    Log.v("Loading complete!")
    pairs
  }

  // Download ALL FreeBase -> WikiData pairs in the entire WikiData db (1.5M~)
  def computeFreeBaseWikiDataPairs() = {
    Log.v("Loading ALL FB -> WD pairs from WikiData ... Hold on!")
    val json = toJson(wmflabs + "claim[646]&props=646") \ "items" \ "646"
    val pairs: Seq[(String, String)] = for {
      JArray(arr) <- json
      JArray(triple) <- arr
      if triple(1).extract[String] == "string"
      List(JInt(wb), JString(valueType), JString(fb)) = triple
    } yield (fb.toString, wb.toString)
    Log.v("Done! Storing ...")
    val file = new File(Config.dataPath + pairsFile)
    file.createNewFile
    val writer = new PrintWriter(file)
    pairs.foreach(p => writer.println(p._1 + "," + p._2))
    writer.close
  }

  def computeGender() = loadWdPropPairs("gender", "21")
  def computeInstanceOf() = loadWdPropPairs("instanceOf", "31")
  def computeOccupation() = loadWdPropPairs("occupation", "106")
  def computeSubclassOf() = loadWdPropPairs("subclassOf", "279")

  def loadPairs(name: String): Map[String, Set[String]] = {
    Config.dataFile("wikidata/" + name).getLines().map(line => {
      val v = line.split(" ")
      (v(0), v(1).split(",").toSet)
    }).toMap
  }

  def addAnnotations(annotationsMap: Map[String, Annotation], pairs: Map[String, Set[String]]): Map[String, Annotation] = {
    val newAnnotations = annotationsMap.values.flatMap(a => {
      // For all annotations ...
      if (pairs.contains(a.wd)) {
        val newAnnotations = pairs.get(a.wd).map(i => i.map(wd => a.fromWd(wd))).get.toList
        Seq(a) ++ newAnnotations
      } else Seq(a)
    })
    val grouped = newAnnotations.groupBy(_.id).map(anns => {
      val mcSum = anns._2.map(_.mc).sum
      (anns._1, anns._2.head.copy(mc = mcSum))
    })
    grouped
  }

  def loadWdPropPairs(name: String, prop: String) = {
    Log.v(s"Loading $name ... Hold on!")
    val json = toJson(wmflabs + s"CLAIM[$prop]%20AND%20CLAIM[646]&props=$prop") \ "props" \ prop
    val pairs: Seq[(String, String)] = for {
      JArray(arr) <- json
      JArray(triple) <- arr
      if triple(1).extract[String] == "item"
      List(JInt(wb), JString(valueType), JInt(instanceOf)) = triple
    } yield (wb.toString, instanceOf.toString)
    Log.v("Done! Storing ...")
    val file = new File(Config.dataPath + "wikidata/" + name + ".txt")
    file.createNewFile
    val writer = new PrintWriter(file)
    pairs.groupBy(_._1).foreach(p => writer.println(p._1 + " " + p._2.map(_._2).mkString(",")))
    writer.close()
  }

  def wdIdToLabel(id: String): String = {
    val json = toJson(s"${wiki}action=wbgetentities&props=labels&ids=Q$id")
    val JString(label) = json \ "entities" \ s"Q$id" \ "labels" \ "en" \ "value"
    label
  }

  def resToId(res: String): String = res.substring(res.lastIndexOf("/") + 1, res.length - 1)

  def extractFreebaseFromWikiDump() = {
    sc.textFile("e:/wikidata-simple-statements.nt")
      .map(_.split(" ").toList.take(3))
      .filter(a => a(1) == "<http://www.wikidata.org/entity/P646c>")
      .map { case a :: b :: c :: Nil => (resToId(a), c.substring(1, c.length - 1)) }
      .map { case (wikiId, fbId) => wikiId + " " + fbId }
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "wiki_to_fb_" + DateTime.now.secondOfDay.get)
  }

  def extractWikiIDFromDbpediaDump() = {
    sc.textFile("e:/wikidatawiki-20150330-sameas-all-wikis.ttl")
      .map(_.split(" ").toList.take(3))
      .filter(a => a(2).startsWith("<http://dbpedia.org"))
      .map { case a :: b :: c :: Nil => (resToId(a), resToId(c)) }
      .map { case (wikiId, dbpediaId) => wikiId + " " + dbpediaId }
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "dbpedia_to_wiki_" + DateTime.now.secondOfDay.get)
  }
}
