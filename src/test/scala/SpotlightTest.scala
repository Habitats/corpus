import no.habitats.corpus.npl.Spotlight
import no.habitats.corpus.{Config, Log}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import util.Samples

/**
  * Created by mail on 09.02.2016.
  */
@RunWith(classOf[JUnitRunner])
class SpotlightTest extends FunSuite with Samples {

  val log = LoggerFactory.getLogger(getClass)

  test("fetch annotations") {
    val test2 = Config.testFile("npl/article.txt").getLines().mkString(" ")
    val ann = Spotlight.fetchAnnotations(test2)
    ann.foreach(Log.v)
  }
}
