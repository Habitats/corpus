/**
  * Created by Patrick on 13.11.2015.
  */

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

  test("print some headlines") {
    val hl = Corpus.articles(count = 10)
      .filter(_.hl != null)
      .sortBy(_.hl)
      .map(Corpus.toIPTC)
      .map(_.toString)
//      .map(a => a.id + " > " + a.hl + " > " + a.iptc.mkString(", ") + " > " + a.url.get)
    Log.v(hl.mkString("\n"))
    Log.v(hl.size + " headlines.")
  }
}
