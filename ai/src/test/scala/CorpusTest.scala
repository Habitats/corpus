/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus._
import no.habitats.corpus.common.models.{Annotation, Article, DBPediaAnnotation, Entity}
import no.habitats.corpus.common.{AnnotationUtils, Config, Log, Spotlight}
import no.habitats.corpus.spark.Fetcher
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import util.Samples

import scala.collection.Map
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class CorpusTest extends FunSuite with Samples {

  test("fetch NYT articles") {
    val raw1 = Corpus.articlesFromXML(Config.testPath + "/nyt/")
    assert(raw1.size == 4)
  }

  test("print some headlines") {
    //      .map(a => a.id + " > " + a.hl + " > " + a.iptc.mkString(", ") + " > " + a.url.get)
    val hl = Corpus.articlesFromXML(count = 10)
      .filter(_.hl != null)
      .sortBy(_.hl)
      .map(Corpus.toIPTC)
      .map(_.toString)
    Log.v(hl.mkString("\n"))
    Log.v(hl.size + " headlines.")
  }

  test("entity serialization bench") {
    import no.habitats.corpus.common.models.Entity
    import no.habitats.corpus.common.{Config, Spotlight}

    val entities = Spotlight.fetchEntities(Config.dataPath + "dbpedia/dbpedia_json_0.25.json")

    val start = System.currentTimeMillis()
    assert(entities.values.forall(e => Entity.fromStringSerialized(Entity.toStringSerialized(e)) == e))
    Log.v("String serialization: " + (System.currentTimeMillis() - start))
    val start2 = System.currentTimeMillis()
    assert(entities.values.forall(e => Entity.fromSingleJson(Entity.toSingleJson(e)) == e))
    Log.v("Json serialization: " + (System.currentTimeMillis() - start2))
  }

  test("dbpedia serialization bench") {
    val r = new Random(123)
    val dbpedia: Seq[DBPediaAnnotation] = for {
      i <- 0 until 10000
      id = r.nextInt.toString
      mc = r.nextInt()
      name = r.nextInt.toString
      eid =r.nextInt.toString
      offset = r.nextInt()
      similarityScore = r.nextFloat()
      support = r.nextInt()
      types = if (r.nextDouble() > 0.5) Set(r.nextInt.toString)  else Set[String]()
    } yield DBPediaAnnotation(id, mc, Entity(eid, name, offset, similarityScore, support, types))

    val start2 = System.currentTimeMillis()
    assert(dbpedia.forall(e => DBPediaAnnotation.fromSingleJson(DBPediaAnnotation.toSingleJson(e)) == e))
    Log.v("Json serialization: " + (System.currentTimeMillis() - start2))

    val start = System.currentTimeMillis()
    assert(dbpedia.forall(e => DBPediaAnnotation.fromStringSerialized(DBPediaAnnotation.toStringSerialized(e)) == e))
    Log.v("String serialization: " + (System.currentTimeMillis() - start))
  }

  test("article serialization bench"){
    val start2 = System.currentTimeMillis()
    val articles = Fetcher.rdd.take(100000)
    val annotations = articles.flatMap(_.ann.values)
    assert(annotations.forall(e => {
      val s: String = Annotation.toStringSerialized(e)
      val e2: Annotation = Annotation.fromStringSerialized(s)
      e2 == e
    }))

    assert(articles.forall(e => JsonSingle.fromSingleJson(JsonSingle.toSingleJson(e)) == e))
    Log.v("Json serialization: " + (System.currentTimeMillis() - start2))

    var i = 0
    val start = System.currentTimeMillis()
    assert(articles.forall(e => {
      val serialized: String = Article.toStringSerialized(e)
      val serialized1: Article = Article.fromStringSerialized(serialized)
      serialized1 == e
    }))
    Log.v("String serialization: " + (System.currentTimeMillis() - start))
  }
}
