/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus.Config
import no.habitats.corpus.npl.Spotlight
import no.habitats.corpus.npl.extractors.{OpenNLP, SimpleStandfordNER}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NLPTest extends FunSuite {

  lazy val test = Config.dataFile("pos/article.txt").getLines().mkString(" ")
  lazy val test2 = Config.dataFile("pos/article_short.txt").getLines().mkString(" ")

  test("extraction") {
    val e1 = OpenNLP.extract(test2)
    val e2 = SimpleStandfordNER.extract(test2)

    println("OpenNLP:")
    e1.foreach(println)

    println("StandfordNER:")
    e2.foreach(println)

    println("Benchmark:")
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
    println(s"OpenNLP: $t ($c) ms > StanfordNER: $t2 ($c2) ms")
  }

  test("dbpedia spotlight") {
    val annotated = Spotlight.fetchAnnotations(test)
  }
}
