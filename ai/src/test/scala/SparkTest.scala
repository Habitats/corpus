import no.habitats.corpus.Corpus
import no.habitats.corpus.common.{Config, DBpediaFetcher, Spotlight}
import no.habitats.corpus.spark.Fetcher
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import util.Spark

@RunWith(classOf[JUnitRunner])
class SparkTest extends FunSuite with Spark {

  test("does it start?") {
    //    sc.setLogLevel("ERROR")
    assert(sc.parallelize(0 until 10).count === 10)
  }

  test("load some articles the old way") {
    val limit = 1000
    val articles = Corpus.articlesFromXML(count = limit)
    val rdd = sc.parallelize(articles)
    assert(rdd.count === limit)
  }

  test("load some articles the new way") {
    val limit = 1000
    //    sc.parallelize(IO.walk(Config.dataPath + "/nyt/", count = limit, filter = ".xml"))
    val confidence: Double = 0.5
    val db = DBpediaFetcher.dbpediaAnnotations(confidence)
    sc.parallelize(
      Fetcher.rdd.take(limit)
    )
      .map(a => Spotlight.toDBPediaAnnotated(a, db))
      .saveAsTextFile(Config.cachePath + "nyt_with_all")
  }
}
