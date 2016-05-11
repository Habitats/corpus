package no.habitats.corpus.common

import dispatch._
import no.habitats.corpus.common.CorpusContext.sc
import no.habitats.corpus.common.models.{Annotation, Article, DBPediaAnnotation, Entity}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.collection.Map
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

  lazy val dbpediaAnnotations       : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpedia)
  lazy val dbpediaAnnotationsMini25 : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini25)
  lazy val dbpediaAnnotationsMini50 : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini50)
  lazy val dbpediaAnnotationsMini75 : Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini75)
  lazy val dbpediaAnnotationsMini100: Map[String, Seq[Annotation]] = fetchDbpediaAnnotations(Config.dbpediaMini100)

  def fetchDbpediaAnnotationsJson(dbpedia: String): Map[String, Seq[Annotation]] = {
    CorpusContext.sc.textFile(dbpedia)
      .map(DBPediaAnnotation.fromSingleJson)
      .map(AnnotationUtils.fromDbpedia)
      //      .filter(an => an.fb != Config.NONE && W2VLoader.contains(an.fb))
      .groupBy(_.articleId)
      .map { case (k, v) => (k, v.toSeq) }
      .collect.toMap
  }

  def fetchDbpediaAnnotations(dbpedia: String): Map[String, Seq[Annotation]] = {
    CorpusContext.sc.textFile(dbpedia)
      .map(DBPediaAnnotation.fromStringSerialized)
      .map(AnnotationUtils.fromDbpedia)
      //      .filter(an => an.fb != Config.NONE && W2VLoader.contains(an.fb))
      .groupBy(_.articleId)
      .map { case (k, v) => (k, v.toSeq) }
      .collect.toMap
  }

  def fetchEntities(file: String): Map[String, Entity] = CorpusContext.sc.textFile(file).map(Entity.fromSingleJson).map(e => (e.id, e)).collectAsMap()

  def fetchArticleMapping(file: String): Map[String, Set[String]] = {
      CorpusContext.sc.textFile(file).flatMap(l => {
        val tokens = l.split(" ")
        val dbpediaId = tokens.head
        val articleIds = tokens.slice(1, tokens.size)
        articleIds.map(id => (id, dbpediaId))
      }).groupBy(_._1).map{case (k,v) => (k, v.map(_._2).toSet)}.collectAsMap()
  }

  lazy val dbpediaAnnotationsWithTypes: Map[String, Seq[Annotation]] = {
    CorpusContext.sc.textFile(Config.dbpedia)
      .map(DBPediaAnnotation.fromSingleJson)
      .flatMap(ann => Seq(AnnotationUtils.fromDbpedia(ann)) ++ AnnotationUtils.fromDBpediaType(ann))
      .groupBy(_.articleId)
      .filter(_._2.nonEmpty)
      .map { case (k, v) => (k, v.toSeq.filter(ann => W2VLoader.contains(ann.fb))) }
      .collect.toMap
  }

  def toDBPediaAnnotated(a: Article, dbpediaAnnotations: Map[String, Seq[Annotation]]): Article = {
    dbpediaAnnotations.get(a.id) match {
      case Some(ann) => a.copy(ann = a.ann ++ ann.map(a => (a.id, a)).toMap)
      case None => Log.v("NO DBPEDIA: " + a.id); a
    }
  }

  def toDBPediaAnnotatedWithTypes(a: Article, dbpediaAnnotations: Map[String, Seq[Annotation]]): Article = {
    dbpediaAnnotationsWithTypes.get(a.id) match {
      case Some(ann) => a.copy(ann = a.ann ++ ann.map(a => (a.id, a)).filter(a => a._2.fb != Config.NONE && W2VLoader.contains(a._1)).toMap)
      case None => Log.v("NO DBPEDIA: " + a.id); a
    }
  }

  def dbpedia(sc: SparkContext, name: String = Config.dbpedia): RDD[DBPediaAnnotation] = {
    val rdd = sc.textFile(name).map(DBPediaAnnotation.fromSingleJson)
    if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(Config.count)) else rdd
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
    val res: String = Await.result(Http(request OK as.String), 15 minutes)
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

  /** Store DBPedia annotations */
  def cacheDbpedia(rdd: RDD[Article], confidence: Double, w2vFilter: Boolean = false) = {
    Log.v("Calculating dbpedia ...")
    val entities = rdd.flatMap { article =>
      for {
        entities <- fetchAnnotations(article.hl + "_" + article.body, confidence).groupBy(_.id).values
        entity = entities.minBy(_.offset) if w2vFilter && WikiData.dbToWd.get(entity.id).flatMap(WikiData.wdToFb.get).exists(W2VLoader.contains)
        db = new DBPediaAnnotation(article.id, mc = entities.size, entity)
      } yield {
        if (Math.random < 0.0001) Log.v(article.id)
        db
      }
    }

    saveAsText(entities.map(DBPediaAnnotation.toStringSerialized), s"dbpedia_string_${confidence}")
//    saveAsText(entities.map(DBPediaAnnotation.toSingleJson), s"dbpedia_json_${confidence}")
  }
}


