import no.habitats.corpus.spark.RddFetcher
import no.habitats.corpus.{Config, Corpus}
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
    sc.parallelize(
      RddFetcher.rdd(sc)
        .take(limit)
    )
      .map(Corpus.toDBPediaAnnotated)
      .saveAsTextFile(Config.cachePath + "nyt_with_all")
  }
}
