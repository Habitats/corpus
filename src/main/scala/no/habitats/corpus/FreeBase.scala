package no.habitats.corpus

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger

import no.habitats.corpus.models.Annotation

import scala.io.Source

object FreeBase {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization

  implicit val formats = Serialization.formats(NoTypeHints)

  val pairsFile = new File("fb_to_wd_all.txt")
  // API roots
  val wmflabs = "http://wdq.wmflabs.org/api?q="
  val wiki = "https://www.wikidata.org/w/api.php?format=json&language=en&"

  lazy val fbToWikiMapping: Map[String, String] = {
    val source = Source.fromFile(pairsFile)
    val pairs = source.getLines().map(_.split(",")).map(p => (p(0), p(1))).toMap
    source.close
    pairs
  }

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
    val file = pairsFile
    file.createNewFile
    val writer = new PrintWriter(file)
    pairs.foreach(p => writer.println(p._1 + "," + p._2))
    writer.close
  }

  def computeInstanceOf() = loadWdPropPairs("instanceOf", "31")

  def computeGender() = loadWdPropPairs("gender", "21")

  def computeOccupation() = loadWdPropPairs("occupation", "106")

  def loadPairs(name: String): Map[String, Set[String]] = Source.fromFile(new File("wikidata/" + name)).getLines().map(line => {
    val v = line.split(" ")
    (v(0), v(1).split(",").toSet)
  }).toMap


  def addAnnotations(annotationsMap: Map[String, Annotation], pairs: Map[String, Set[String]], broadOnly: Boolean): Map[String, Annotation] = {
    def annotationFromWd(oldAnnotation: Annotation, wd: String, index: Int): Annotation = {
      Annotation(
        salience = 1,
        index = index,
        broad = true,
        offset = oldAnnotation.offset,
        articleId = oldAnnotation.articleId,
        wd = wd,
        phrase = wd + " - " + oldAnnotation.phrase,
        mc = oldAnnotation.mc
      )
    }

    def createNewAnnotations(a: Annotation, oldAnnotation: Annotation): Seq[Annotation] = {
      val counter = new AtomicInteger(oldAnnotation.index)
      val newAnnotations = pairs.get(oldAnnotation.wd).map(i => i.map(wd => {
        // Create annotation from WikiDAta ID
        val newAnnotation = annotationFromWd(oldAnnotation, wd, counter.getAndIncrement)
        newAnnotation
      })).get.toList

      // Decide whether to return old and new, or just the new
      if (broadOnly) {
        newAnnotations
      } else {
        Seq(a) ++ newAnnotations
      }
    }

    val newAnnotations = annotationsMap.flatMap(a => {
      // For all annotations ...
      val oldAnnotation = a._2
      if (pairs.contains(oldAnnotation.wd)) {
        createNewAnnotations(a._2, oldAnnotation)
      } else {
        Seq(a._2)
      }
    })
    val grouped = newAnnotations.groupBy(_.id).map(anns => {
      val mcSum = anns._2.map(_.mc).sum
      (anns._1, anns._2.head.copy(mc = mcSum))
    })
    grouped
  }

  val instanceOf = loadPairs("instanceOf.txt")
  val occupations = loadPairs("occupation.txt")
  val genders = loadPairs("gender.txt")

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
    val file = new File("wikidata/" + name + ".txt")
    file.createNewFile
    val writer = new PrintWriter(file)
    pairs.groupBy(_._1).foreach(p => writer.println(p._1 + " " + p._2.map(_._2).mkString(",")))
    writer.close()
  }

  def instanceOf(id: String): String = {
    val json = toJson(s"${wmflabs}tree[$id][31]")
    val JArray(List(JInt(a), JInt(_), JInt(_))) = json \ "items"
    a.toString
  }

  def wdToString(wd: String): String = {
    val json = toJson(s"${wiki}action=wbgetentities&props=labels&ids=Q$wd")
    val JString(label) = json \ "entities" \ s"Q$wd" \ "labels" \ "en" \ "value"
    label
  }

  def writeLine(m: String, file: File) = {
    synchronized {
      val writer = new PrintWriter(new FileOutputStream(file, true))
      writer.println(m)
      writer.close()
    }
  }

  implicit def toJson(url: String): JValue = parse(Source.fromURL(url).mkString)

}
