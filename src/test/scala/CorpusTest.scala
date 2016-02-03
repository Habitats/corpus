/**
  * Created by Patrick on 13.11.2015.
  */

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.features.WikiData
import no.habitats.corpus.spark.Preprocess._
import no.habitats.corpus.spark.{Context, Preprocess}
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
    val annotations = Corpus.annotations(files = Corpus.walk(new File(Config.testPath + testCache), ".txt"))
    val articles = Corpus.annotatedArticles(raw, annotations)
    articles.toSeq
  }

  test("relevant filter") {
    val raw1 = Corpus.articles(files = testFiles)
    val raw2 = Corpus.articles(files = Corpus.walk(new File(Config.testPath + testCache), ".xml", Set("1818263", "1822395")))

    assert(raw1.size == 4)
    assert(raw2.size == 2)
  }

  test("count iptc") {
    val a = Preprocess.computeIptc(sc.parallelize(articles.take(2)), true)
    val yo = a.collect
    val annCounts = a.flatMap(a => a.iptc.map(c => (c, a.ann.size))).reduceByKey(_ + _).collectAsMap
    Log.toFile(annCounts.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "annotation_pr_iptc")
    val artByAnn = a.flatMap(a => a.iptc.map(c => (c, 1))).reduceByKey(_+ _).collectAsMap
    Log.toFile(artByAnn.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "articles_pr_iptc")
    val iptc = a.flatMap(_.iptc).distinct.collect
    val avgAnnIptc = iptc.map(c => (c, annCounts(c).toDouble / artByAnn(c))).toMap
    Log.toFile(avgAnnIptc.map(c => f"${c._1}%30s ${c._2}%10.0f").mkString("\n"), "average_ann_pr_iptc")
    annCounts.foreach(println)
  }

  test("spark works") {
    val rddNoIptc = sc.parallelize(articles)
    val rdd = computeIptc(rddNoIptc, true)
    assert(rdd.count == 4)
    val pre = preprocess(sc, sc.broadcast(Prefs()), rdd)
    //    pre.foreach(println)
  }

  test("print stuff") {
    val prefs = Prefs(
      wikiDataOnly = false,
      termFrequencyThreshold = 2
    )

    val superRaw = Corpus.rawArticles(files = testFiles)
    superRaw.foreach(println)
    val rdd = sc.parallelize(articles)
    val raw = rdd.collect
    raw.foreach(a => println(a.toStringFull))
    val processed = preprocess(sc, sc.broadcast(prefs), rdd).collect
    processed.foreach(a => println(a.toStringFull))
    //    processed.flatMap(_.ann.values).sortBy(_.tfIdf).foreach(a => println(a))

    val phrases = processed.flatMap(_.ann.keySet).distinct.toSeq.sorted
    println(phrases.map(p => f"$p%-200s").map(_.substring(0, 15)).mkString(f"${"Freebase"}%-15s", "  ", ""))
    println(phrases.map(p => if (WikiData.fbToWikiMapping.contains(p)) "Q" + WikiData.fbToWikiMapping(p) else "").map(p => f"$p%-200s").map(_.substring(0, 15)).mkString(f"${"WikiData"}%-15s", "  ", ""))
    println(phrases.map(p => if (WikiData.fbToWikiMapping.contains(p)) WikiData.wdToString(WikiData.fbToWikiMapping(p)) else "").map(p => f"${p}%-200s").map(_.substring(0, 15)).mkString(f"${"Phrase"}%-15s", "  ", ""))
    processed.map(a => a.toVector(phrases)).map(_.toArray).map(_.map(d => f"$d%.3f").map(p => f"${p}%-15s").mkString(f"${"Article "}%-15s", "  ", "")).foreach(println)
  }

  test("evaluation") {
    //    articles.
    //    val rdd = sc.parallelize(articles)
    //    ML.
  }
}
