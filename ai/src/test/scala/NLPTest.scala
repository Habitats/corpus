/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus.common.Config
import no.habitats.corpus.npl.extractors.{OpenNLP, SimpleStandfordNER}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class NLPTest extends FunSuite {

  val log = LoggerFactory.getLogger(getClass)
  lazy val test = Config.testFile("npl/article.txt").getLines().mkString(" ")
  lazy val test2 = Config.testFile("npl/article_short.txt").getLines().mkString(" ")

  test("extraction") {
    val e1 = OpenNLP.extract(test2)
    val e2 = SimpleStandfordNER.extract(test2)

    log.info("OpenNLP:")
    e1.foreach(println)

    log.info("StandfordNER:")
    e2.foreach(println)

    log.info("Benchmark:")
    val num = 10
    val goal = 1800000
    val s = System.currentTimeMillis()
    (0 to num).foreach(i => OpenNLP.extract(test))
    val t = System.currentTimeMillis() - s

    val s2 = System.currentTimeMillis()
    (0 to num).foreach(i => SimpleStandfordNER.extract(test))
    val t2 = System.currentTimeMillis() - s2

    val c = ((goal / num) * t) / (1000 * 60 * 24)
    val c2 = ((goal / num) * t2) / (1000 * 60 * 24)
    log.info(s"OpenNLP: $t ($c) ms > StanfordNER: $t2 ($c2) ms")
  }
}
