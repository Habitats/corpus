package no.habitats.corpus.common

import dispatch._
import no.habitats.corpus.common.CorpusContext.sc
import no.habitats.corpus.common.CorpusExecutionContext.executionContext
import no.habitats.corpus.common.models.{Annotation, Article, DBPediaAnnotation, Entity}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.concurrent.Await
import scala.concurrent.duration._

object DBpediaFetcher {

  def dbpedia(confidence: Double, json: Boolean = false): RDD[DBPediaAnnotation] = {
    val name = Config.dataPath + s"dbpedia/dbpedia_all_$confidence.${if (json) "json" else "txt"}"
    val rdd = sc.textFile("file:///" + name)
      .map(s => if (json) DBPediaAnnotation.fromSingleJson(s) else DBPediaAnnotation.deserialize(s))
    if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(Config.count)) else rdd
  }

  private def fetchDbpediaAnnotations(confidence: Double, json: Boolean, types: Boolean): Map[String, Seq[Annotation]] = {
    dbpedia(confidence, json)
      .flatMap(ann => if (types) Seq(AnnotationUtils.fromDbpedia(ann)) else Nil ++ AnnotationUtils.fromDBpediaType(ann))
      .filter(an => an.fb != Config.NONE && W2VLoader.contains(an.fb))
      .map(a => (a.articleId, Seq(a)))
      .reduceByKey(_ ++ _)
      .collect.toMap
  }

  def dbpediaAnnotations(confidence: Double, types: Boolean = false, json: Boolean = false): Map[String, Seq[Annotation]] = {
    fetchDbpediaAnnotations(confidence, json, types)
  }
}

object Spotlight extends RddSerializer {

  implicit val formats = Serialization.formats(NoTypeHints)

  /** Combine and store annotations with DBpedia, Wikidata and Freebase ID's */
  def combineAndCacheIds(confidence: Double = 0.5) = {
    val dbp = WikiData.dbToWd
    val dbf = WikiData.wdToFb
    val rdd = DBpediaFetcher.dbpedia(confidence)
      .map(_.entity.id)
      .map(a => (a, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .map { case (db, count) =>
        val wd = dbp.get(db)
        val fb = dbf.get(wd.getOrElse(""))
        f"$count%-8s ${wd.getOrElse("")}%-10s ${fb.getOrElse("")}%-12s $db"
      }

    saveAsText(rdd, Config.cachePath + "combined_ids.txt")
  }

  def fetchEntities(file: String): Map[String, Entity] = CorpusContext.sc.textFile("file:///" + file).map(Entity.fromSingleJson).map(e => (e.id, e)).collect.toMap

  def fetchArticleMapping(file: String): Map[String, Set[String]] = {
    CorpusContext.sc.textFile("file:///" + file).flatMap(l => {
      val tokens = l.split(" ")
      val articleIds = tokens.slice(1, tokens.size)
      val dbpediaId = tokens.head
      articleIds.map(id => (id, dbpediaId))
    }).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).toSet) }.collect.toMap
  }

  def toDBPediaAnnotated(a: Article, db: Map[String, Seq[Annotation]]): Option[Article] = {
    db.get(a.id) match {
      case Some(ann) => Some(a.copy(ann = (a.ann ++ ann.map(a => (a.id, a))).filter(a => W2VLoader.contains(a._1))))
      case _ => None

      /** Log.v("NO DBPEDIA: " + a.id); */
    }
  }

  /** Entity extraction using DBPedia Spotlight REST API */
  def fetchAnnotations(text: String, confidence: Double = 0.5): List[Entity] = {
    val request: Req = url(Config.dbpediaSpotlightURL).POST
      .addHeader("content-type", "application/x-www-form-urlencoded")
      .addHeader("Accept", "application/json")
      .addParameter("User-agent", Math.random.toString)
      .addParameter("text", text)
      .addParameter("confidence", confidence.toString)
    //    .addParameter("types", "Person,Organisation,Location")

    // Let Spark handle the parallelism, it's pretty good at it
    val res: String = Await.result(Http(request OK as.String), 60 days)
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
      Entity(ex(uri).split("/").last, ex(name), ex(offset).toInt, ex(similarityScore).toFloat, ex(support).toInt, ex(types).split(",|:").filter(_.startsWith("Q")).toSet)
    }

    entities
  }

  def extractDbpediaAnnotations(article: Article, confidence: Double, w2vFilter: Boolean): Seq[DBPediaAnnotation] = {
    val all = for {
      firstEntities <- fetchAnnotations(article.hl + "_" + article.body, confidence).groupBy(_.id).values.toSeq
      entity = firstEntities.minBy(_.offset) if w2vFilter && WikiData.dbToWd.get(entity.id).flatMap(WikiData.wdToFb.get).exists(W2VLoader.contains)
      db = new DBPediaAnnotation(article.id, mc = firstEntities.size, entity)
    } yield db
    Log.v(s"Extracted ${all.size} from ${article.id} ...")
    all
  }

  /** Store DBPedia annotations */
  def cacheDbpedia(rdd: RDD[Article], confidence: Double, w2vFilter: Boolean = false, partition: Option[Int]) = {
    Log.v("Calculating dbpedia ...")
    val dbpediaAnnotations = rdd.flatMap(a => extractDbpediaAnnotations(a, confidence, w2vFilter))
    saveAsText(dbpediaAnnotations.map(DBPediaAnnotation.serialize), s"dbpedia_string_${confidence}${partition.map("_" + _).getOrElse("")}")
  }
}
