package no.habitats.corpus.npl

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger

import no.habitats.corpus.models.Annotation
import no.habitats.corpus.{Config, Log}
import org.apache.spark.SparkContext
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
  val wmflabs = "http://wdq.wmflabs.org/api?q="
  val wiki = "https://www.wikidata.org/w/api.php?format=json&language=en&"

  lazy val instanceOf = loadPairs("instanceOf.txt")
  lazy val occupations = loadPairs("occupation.txt")
  lazy val genders = loadPairs("gender.txt")

  lazy val fbToWikiMapping: Map[String, String] = Config.dataFile(pairsFile).getLines().map(_.split(",")).map(p => (p(0), p(1))).toMap
  lazy val wikiToFbMapping: Map[String, String] = fbToWikiMapping.map(p => (p._2, p._1))

  // Perform a single Freebase -> WikiData translation
  def fbToWiki(freeBaseId: String): Option[String] = {
    val json = toJson(s"${wmflabs}string[646:$freeBaseId]")
    val items = render(json \ "items").children
    val wd = if (items.nonEmpty) Some("Q" + items.head.extract[Int]) else None
    Log.v(f"FB -> WD: $freeBaseId%-10s -> ${wd.getOrElse("NONE")}")
    wd
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
        val counter = new AtomicInteger(a.index)
        val newAnnotations = pairs.get(a.wd).map(i => i.map(wd => a.fromWd(counter.getAndIncrement, wd))).get.toList
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

  def writeLine(m: String, file: File) = {
    synchronized {
      val writer = new PrintWriter(new FileOutputStream(file, true))
      writer.println(m)
      writer.close()
    }
  }

  def extractFbFromWikiDump(sc: SparkContext) = {
    //    sc.parallelize(
    sc.textFile("e:/wikidata-simple-statements.nt")
      //        .take(1000)
      //    )
      .map(_.split(" ").toList.take(3))
      .filter(a => a(1) == "<http://www.wikidata.org/entity/P646c>")
      .map { case a :: b :: c :: Nil => (a.substring(a.lastIndexOf("/") + 1, a.length - 1), c.substring(1, c.length - 1)) }
      .map { case (wikiId, fbId) => wikiId + " " + fbId }
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "wiki_to_fb_" + DateTime.now.secondOfDay.get)
  }

  def extractWdFromDbpediaPropsDump(sc: SparkContext) = {
    sc.textFile("e:/infobox-properties_en.nt")
      .map(_.split(" ").toList.take(3))
      .filter(a => a(1) == "<http://dbpedia.org/property/d>")
      .map { case a :: b :: c :: Nil => (resToId(a), c.substring(1, c.length - 4)) }
      .map { case (dbpediaId, wikiId) => wikiId + " " + dbpediaId }
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "dbpedia_to_wiki_" + DateTime.now.secondOfDay.get)
  }

  def resToId(res: String): String = res.substring(res.lastIndexOf("/") + 1, res.length - 1)

  def extractWdFromDbpediaSameAsDump(sc: SparkContext) = {
    sc.textFile("e:/wikidatawiki-20150330-sameas-all-wikis.ttl")
      .map(_.split(" ").toList.take(3))
      .filter(a => a(2).startsWith("<http://dbpedia.org"))
      .map { case a :: b :: c :: Nil => (resToId(a), resToId(c)) }
      .map { case (wikiId, dbpediaId) => wikiId + " " + dbpediaId }
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "dbpedia_to_wiki_" + DateTime.now.secondOfDay.get)
  }
}
