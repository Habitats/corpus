/**
  * Created by Patrick on 13.11.2015.
  */

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.spark.Context
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class CorpusTest extends FunSuite {

  val testCache = "test"
  lazy val testFiles = Corpus.walk(new File(Config.testPath + testCache), ".xml")
  lazy val sc = Context.localContext
  lazy val articles = {
    val raw = Corpus.articles(files = testFiles)
    val annotations = Corpus.annotations()
    val articles = Corpus.annotatedArticles(raw, annotations)
    articles.toSeq
  }

  test("relevant filter") {
    val raw1 = Corpus.articles(files = testFiles)
    val raw2 = Corpus.articles(files = Corpus.walk(new File(Config.testPath + testCache), ".xml", Set("1818263", "1822395")))

    assert(raw1.size == 4)
    assert(raw2.size == 2)
  }
}
