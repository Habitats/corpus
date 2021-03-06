
/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.nlp.extractors.{OpenNLP, Simple}
import no.habitats.corpus.npl.extractors.SimpleStandfordNER
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class NLPTest extends FunSuite {

  lazy val test  = Config.testFile("nlp/article.txt").getLines().mkString(" ")
  lazy val test2 = Config.testFile("nlp/article_short.txt").getLines().mkString(" ")

  test("tokenize") {
    val t1 = OpenNLP.tokenize(test2)
    val t2 = SimpleStandfordNER.tokenize(test2)
    val t3 = Simple.tokenize(test2)
    Log.v(t1.mkString(" | "))
    Log.v(t2.mkString(" | "))
    Log.v(t3.mkString(" | "))
  }

  test("extraction") {
    val e1 = OpenNLP.extract(test2)
    val e2 = SimpleStandfordNER.extract(test2)

    Log.v("OpenNLP:")
    e1.foreach(println)

    Log.v("StandfordNER:")
    e2.foreach(println)

    Log.v("Benchmark:")
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
    Log.v(s"OpenNLP: $t ($c) ms > StanfordNER: $t2 ($c2) ms")
  }
}

