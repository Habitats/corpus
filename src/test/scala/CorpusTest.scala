/**
  * Created by Patrick on 13.11.2015.
  */

import java.io.File

import no.habitats.corpus._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import util.Samples


@RunWith(classOf[JUnitRunner])
class CorpusTest extends FunSuite with Samples {

  test("fetch NYT articles") {
    val raw1 = Corpus.articles(Config.testPath + "/nyt/")
    assert(raw1.size == 4)
  }
}
