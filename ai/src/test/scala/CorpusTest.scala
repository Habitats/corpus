/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus._
import no.habitats.corpus.common.{Config, Log}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import util.Samples

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
}
