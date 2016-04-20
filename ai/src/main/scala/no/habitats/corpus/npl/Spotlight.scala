package no.habitats.corpus.npl

import dispatch._

import no.habitats.corpus.common.CorpusContext.sc
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.models.{Article, DBPediaAnnotation, Entity}
import no.habitats.corpus.spark.{RddFetcher, SparkUtil}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Spotlight {

  implicit val formats = Serialization.formats(NoTypeHints)

  /** Combine and store annotations with DBpedia, Wikidata and Freebase ID's */
  def combineAndCacheIds() = {
    val dbp = WikiData.dbToWd
    val dbf = WikiData.wdToFb
    val rdd = RddFetcher.dbpedia(sc)
      .map(_.entity.id)
      .map(a => (a, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map { case (db, count) =>
        val wd = dbp.get(db)
        val fb = dbf.get(wd.getOrElse(""))
        f"$count%-8s ${wd.getOrElse("")}%-10s ${fb.getOrElse("")}%-12s $db"
      }

    SparkUtil.saveAsText(rdd, Config.dbpedia)
  }

  /** Entity extraction using DBPedia Spotlight REST API */
  def fetchAnnotations(text: String, confidence: Double = 0.5): List[Entity] = {
    val request = url(Config.dbpediaSpotlightURL).POST
      .addHeader("content-type", "application/x-www-form-urlencoded")
      .addHeader("Accept", "application/json")
      .addParameter("User-agent", Math.random.toString)
      .addParameter("text", text)
      .addParameter("confidence", confidence.toString)
//    .addParameter("types", "Person,Organisation,Location")

    // Let Spark handle the parallelism, it's pretty good at it
    val res = Await.result(Http(request OK as.String), 15 minutes)
    val json = parse(res)
//    Log.v(pretty(json))
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
      Entity(uri.split("/").last, name, offset.toInt, similarityScore.toDouble, support.toInt, types.split(",|:").filter(_.startsWith("Q")).toSet)
    }

    entities
  }

  /** Store DBPedia annotations */
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

    SparkUtil.saveAsText(entities, "dbpedia_json_" + confidence)
  }
}


