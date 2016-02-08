package no.habitats.corpus.npl

import dispatch.Defaults._
import dispatch._
import no.habitats.corpus.{Corpus, Config}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

object Spotlight {
  implicit val formats = Serialization.formats(NoTypeHints)

  val log = LoggerFactory.getLogger(getClass)

  val root = "http://localhost:2222/rest"
  val dbpediaSparql = "http://dbpedia.org/sparql?"

  def main(args: Array[String]): Unit = {
    val test2 = Config.dataFile("pos/article.txt").getLines().mkString(" ")
    val articles = Corpus.articles().find(_.id == "1453144")

    for {
      article <- articles
    } yield for {
      dbPedias <- fetchAnnotations(article.body)
    } yield for {
      dbPedia <- dbPedias
    } yield for {
      wd <- fetchSameAs(dbPedia)
    } yield {
      log.info(article.id + " > " + dbPedia + " -> " + wd)
    }
  }

  def fetchSameAs(entity: Entity): Future[Entity] = {
    val query =
      f"""
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |PREFIX : <http://dbpedia.org/resource/>
         |SELECT ?u WHERE{<${entity.uri}> owl:sameAs ?u.FILTER regex(str(?u),"wikidata.org")}
         | """.stripMargin
    val request = url(dbpediaSparql).GET
      //      .addHeader("content-type", "application/x-www-form-urlencoded")
      .addHeader("Accept", "application/json")
      .addQueryParameter("query", query)
      .addParameter("format", "application/json")
    for {
      res <- Http(request OK as.String)
      json = parse(res)
      JString(uri) = json \ "results" \ "bindings" \ "u" \ "value"
      id = uri.split("/").last
    } yield {
      Entity(id, entity.name, uri)
    }
  }

  def fetchAnnotations(text: String): Future[Seq[Entity]] = {
    val request = url(root + "/annotate").POST
      .addHeader("content-type", "application/x-www-form-urlencoded")
      .addHeader("Accept", "application/json")
      .addParameter("text", text)
      .addParameter("confidence", "0.5")
      .addParameter("types", "Person,Organisation,Location")
    val res = Http(request OK as.String)
    for (c <- res) yield {
      val json = parse(c)
      val JArray(resources) = json \ "Resources"
      val entities = for {
        resource <- resources
        JString(uri) = resource \ "@URI"
        JString(name) = resource \ "@surfaceForm"
      } yield {
        Entity(uri.split("/").last, name, uri)
      }

      entities
    }
  }
}

case class Entity(id: String, name: String, uri: String)
