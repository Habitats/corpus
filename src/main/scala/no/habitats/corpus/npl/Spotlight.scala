package no.habitats.corpus.npl

import java.io.{File, PrintWriter}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import dispatch._
import no.habitats.corpus.models.{Annotation, Article, Entity}
import no.habitats.corpus.{Config, Corpus, DBPediaAnnotation, Log}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object Spotlight {

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

  def main(args: Array[String]): Unit = {
    //    val articles = Corpus.articles(count = 100)
    cache()
  }

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
      dbPedia <- fetchAnnotations(text)
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
      Entity(id, entity.name, uri)
    }
  }

  def fetchAnnotations(text: String): Future[Seq[Entity]] = {
    val request = url(root + "/annotate").POST
      .addHeader("content-type", "application/x-www-form-urlencoded")
      .addHeader("Accept", "application/json")
      .addParameter("User-agent", Math.random.toString)
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

  def cache() = {
    val f = new File(Config.dataPath + "/nyt/dbpedia.json")
    val start = System.currentTimeMillis()
    val count = new AtomicInteger(0)
    val p = new PrintWriter(f, "iso-8859-1")
    val allf = Corpus.articles(count = Config.count).map { e =>
      if (count.get % 100 == 0) {
        val delta = (System.currentTimeMillis() - start).toDouble
        val perSecond = count.get / delta
        val timesLeft = ((perSecond.toDouble * (Config.count - count.get)) / 1000).toInt
        Log.v(f"$count%10s - $timesLeft%10d - $perSecond%10f")
      }
      count.incrementAndGet()
      Spotlight.extractAndCache(e, p)
    }
    Future.sequence(allf).onComplete { case _ => p.close }

  }

  def extractAndCache(article: Article, p: PrintWriter): Future[Seq[DBPediaAnnotation]] = {
    val f = for {
      entities <- fetchAnnotations(article.hl + " " + article.body)
    } yield for {
      entity <- entities
    } yield {
      val db = new DBPediaAnnotation(article.id, entity)
      val json = DBPediaAnnotation.toSingleJson(db)
      Log.v(json)
      p.println(json)
      db
    }

    f
  }
}


