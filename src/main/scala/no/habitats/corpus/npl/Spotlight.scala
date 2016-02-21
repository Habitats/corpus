package no.habitats.corpus.npl

import java.util.concurrent.Executors

import dispatch._
import no.habitats.corpus.models.{Annotation, Article, DBPediaAnnotation, Entity}
import no.habitats.corpus.{Config, Log}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object Spotlight {

  def combineIds(sc: SparkContext) = {
    val wdToFb = sc.textFile(Config.dataPath + "wikidata/fb_to_wd_all.txt").map(_.split(" ")).map(a => (a(0), a(1))).collectAsMap()
    val dbToWd = sc.textFile(Config.dataPath + "wikidata/dbpedia_to_wikidata.txt").map(_.split(" ")).filter(_.length == 2).map(a => (a(1), a(0))).collectAsMap()
    val rdd = sc.textFile(Config.dataPath + "nyt/dbpediaids_all_0.5.txt").map(_.split("\\s+").toList)
      .map { case count :: db :: Nil =>
        val wd = dbToWd.get(db)
        val fb = wdToFb.get(wd.getOrElse(""))
        f"$count%-10s ${wd.getOrElse("")}%-10s ${fb.getOrElse("")}%-12s $db"
      }
      .coalesce(1, shuffle = true)
      .saveAsTextFile(Config.cachePath + "combined_ids" + DateTime.now.secondOfDay.get)
  }

  implicit val formats = Serialization.formats(NoTypeHints)
  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(10);

    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable) {}
  }

  val log = LoggerFactory.getLogger(getClass)

  val root = "http://localhost:2222/rest"
  val dbpediaSparql = "http://dbpedia.org/sparql?"

  def attachWikidata(articles: Seq[Article]): Future[Seq[Article]] = {
    Future.sequence(articles.map(attachWikidata))
  }

  def attachWikidata(article: Article): Future[Article] = {
    extractWikidata(article.body)
      .map(entities => entities.seq.map(_._2))
      .map(wdEntities => wdEntities.map(wikidata => Annotation.fromWikidata(article.id, wikidata)))
      .map(ann => {
        val mapped = ann.map(_.id).zip(ann).toMap
        mapped
      })
      .map(annotations => article.copy(ann = annotations))
  }

  def extractWikidata(text: String): Future[Seq[(Entity, Entity)]] = {
    for {
      dbPedia <- fetchAnnotationsAsync(text)
      wd <- Future.sequence(dbPedia.map(fetchSameAs)).recover { case f =>
        log.error("Couldn't fetch Wikidata ... Using DBPedia. Error: " + f.getMessage)
        dbPedia
      }
    } yield dbPedia.zip(wd)
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
      entity.copy(id = id, name = entity.name, uri = uri)
    }
  }

  def fetchAnnotationsAsync(text: String, confidence: Double = 0.5): Future[Seq[Entity]] = {
    val request = url(root + "/annotate").POST
      .addHeader("content-type", "application/x-www-form-urlencoded")
      .addHeader("Accept", "application/json")
      .addParameter("User-agent", Math.random.toString)
      .addParameter("text", text)
      .addParameter("confidence", confidence.toString)
    //      .addParameter("types", "Person,Organisation,Location")
    val res = Http(request OK as.String)
    for (c <- res) yield {
      val json = parse(c)
      Log.v(pretty(json))
      val resources = json \ "Resources" match {
        case JArray(e) => e
        case _ => Nil
      }
      val entities = for {
        resource <- resources
        JString(uri) = resource \ "@URI"
        JString(offset) = resource \ "@offset"
        JString(similarityScore) = resource \ "@similarityScore"
        JString(types) = resource \ "@types"
        JString(support) = resource \ "@support"
        JString(name) = resource \ "@surfaceForm"
      } yield {
        Entity(uri.split("/").last, name, uri, offset.toInt, similarityScore.toDouble, support.toInt, types.split(",|:").filter(_.startsWith("Q")).toSet)
      }

      entities
    }
  }

  def fetchAnnotations(text: String, confidence: Double = 0.4): Seq[Entity] = {
    Await.result(fetchAnnotationsAsync(text, confidence), 15 minutes)
  }

  def cacheDbpedia(rdd: RDD[Article], confidence: Double) = {
    Log.v("Calculating dbpedia ...")
    val entities = rdd.flatMap { article =>
      for {
        entities <- fetchAnnotations(article.hl + "_" + article.body, confidence).groupBy(_.id).values
        db = new DBPediaAnnotation(article.id, mc = entities.size, entities.minBy(_.offset))
        json = DBPediaAnnotation.toSingleJson(db)
      } yield {
        if (Math.random < 0.0001) Log.v(article.id)
        json
      }
    }
    entities.coalesce(1, shuffle = true).saveAsTextFile(Config.cachePath + "dbpedia_json_" + confidence + "_" + DateTime.now.secondOfDay.get)
  }

  def cacheDbpediaIds(rdd: RDD[DBPediaAnnotation]) = {
    rdd.map(_.entity.id).map(a => (a, 1)).reduceByKey(_ + _).sortBy(_._2).map { case (id, count) => f"$count%-8d $id" }.coalesce(1).saveAsTextFile(Config.cachePath + Config.dbpedia + "_" + DateTime.now.secondOfDay.get)
  }
}


