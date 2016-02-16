import no.habitats.corpus.npl.Spotlight
import no.habitats.corpus.{Config, Corpus, DBPediaAnnotation, Log}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import util.Samples

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by mail on 09.02.2016.
  */
@RunWith(classOf[JUnitRunner])
class SpotlightTest extends FunSuite with Samples {

  import scala.concurrent.ExecutionContext.Implicits.global

  val log = LoggerFactory.getLogger(getClass)

  test("attach wikidata") {
    Spotlight.attachWikidata(articles).onComplete {
      case Success(a) => a.map(_.toStringFull).foreach(log.info)
      case Failure(ex) => log.error(ex.getMessage, ex)
    }
  }

  test("fetch annotations") {
    val test2 = Config.testFile("npl/article.txt").getLines().mkString(" ")
    Spotlight.fetchAnnotations(test2).onComplete {
      case Success(s) => log.info(s.toString)
      case Failure(ex) => log.error(ex.getMessage, ex)
    }
  }
}
