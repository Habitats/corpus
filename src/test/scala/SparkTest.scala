import no.habitats.corpus.{Config, Corpus, IO}
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
    val articles = Corpus.articles(count = limit)
    val annotated = Corpus.annotatedArticles(articles)
    val rdd = sc.parallelize(annotated)
    assert(rdd.count === limit)
  }

  test("load some articles the new way") {
    val limit = 1000
    val rdd = sc.parallelize(IO.walk(Config.dataPath + "/nyt/", count = limit, filter = ".xml"))
      .map(Corpus.toNYT)
      .map(Corpus.toArticle)
      .map(Corpus.toAnnotated)
    assert(rdd.count === limit)
  }
}
