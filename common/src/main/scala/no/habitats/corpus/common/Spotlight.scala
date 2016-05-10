package no.habitats.corpus.common

import dispatch._
import no.habitats.corpus.common.CorpusContext.sc
import no.habitats.corpus.common.models.{Article, DBPediaAnnotation, Entity}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Spotlight extends RddSerializer {

  implicit val formats = Serialization.formats(NoTypeHints)

  /** Combine and store annotations with DBpedia, Wikidata and Freebase ID's */
  def combineAndCacheIds() = {
    val dbp = WikiData.dbToWd
    val dbf = WikiData.wdToFb
    val rdd = dbpedia(sc)
      .map(_.entity.id)
      .map(a => (a, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map { case (db, count) =>
        val wd = dbp.get(db)
        val fb = dbf.get(wd.getOrElse(""))
        f"$count%-8s ${wd.getOrElse("")}%-10s ${fb.getOrElse("")}%-12s $db"
      }

    saveAsText(rdd, Config.dbpedia)
  }

  def dbpedia(sc: SparkContext, name: String = Config.dbpedia): RDD[DBPediaAnnotation] = {
    val rdd = sc.textFile(name).map(DBPediaAnnotation.fromSingleJson)
    if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(Config.count)) else rdd
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
    def ex(j: Any): String = {val JString(s) = j; s}
    val entities = for {
      resource <- resources
      uri <- resource \ "@URI" if uri != JNull
      offset <- resource \ "@offset" if offset != JNull
      similarityScore <- resource \ "@similarityScore" if similarityScore != JNull
      types <- resource \ "@types" if types != JNull
      support <- resource \ "@support" if support != JNull
      name <- resource \ "@surfaceForm" if name != JNull
    } yield {
      Entity(ex(uri).split("/").last, ex(name), ex(offset).toInt, ex(similarityScore).toDouble, ex(support).toInt, ex(types).split(",|:").filter(_.startsWith("Q")).toSet)
    }

    entities
  }

  /** Store DBPedia annotations */
  def cacheDbpedia(rdd: RDD[Article], confidence: Double, w2vFilter: Boolean = false) = {
    Log.v("Calculating dbpedia ...")
    val entities = rdd.flatMap { article =>
      for {
        entities <- fetchAnnotations(article.hl + "_" + article.body, confidence).groupBy(_.id).values
        entity = entities.minBy(_.offset) if w2vFilter && WikiData.dbToWd.get(entity.id).flatMap(WikiData.wdToFb.get).exists(W2VLoader.contains)
        db = new DBPediaAnnotation(article.id, mc = entities.size, entity)
        json = DBPediaAnnotation.toSingleJson(db)
      } yield {
        if (Math.random < 0.0001) Log.v(article.id)
        json
      }
    }

    saveAsText(entities, "dbpedia_json_" + confidence)
  }
}


