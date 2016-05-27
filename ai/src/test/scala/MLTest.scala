/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext
import no.habitats.corpus.common.models.{Annotation, Article}
import no.habitats.corpus.mllib.{ExampleBased, MacroAverage, Measure, MicroAverage}
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalactic.TolerantNumerics
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MLTest extends FunSuite {
  val epsilon = 1e-2f

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  val A = "a"
  val B = "b"
  val C = "c"
  val D = "d"

  val cats = Set(A, B, C, D)

  val abcd = Set(A, B, C, D)
  val abc  = Set(A, B, C)
  val abd  = Set(A, B, D)
  val acd  = Set(A, C, D)
  val bcd  = Set(B, C, D)
  val ab   = Set(A, B)
  val ac   = Set(A, C)
  val ad   = Set(A, D)
  val bc   = Set(B, C)
  val bd   = Set(B, D)
  val cd   = Set(C, D)
  val a    = Set(A)
  val b    = Set(B)
  val c    = Set(C)
  val d    = Set(D)

  // T + T = TP
  // T + F = FN
  // F + F = TN
  // F + T = FP

  test("multi label") {
    val pred: RDD[Article] = CorpusContext.sc.parallelize(Seq(
      Article(id = "a1", iptc = abc, pred = acd),
      Article(id = "a2", iptc = acd, pred = acd),
      Article(id = "a3", iptc = bcd, pred = acd),
      Article(id = "a4", iptc = abd, pred = acd),
      Article(id = "a5", iptc = ad, pred = acd),
      Article(id = "a6", iptc = ac, pred = acd),
      Article(id = "a7", iptc = d, pred = acd),
      Article(id = "a8", iptc = a, pred = acd)
    ))

    val mi = MicroAverage(pred, cats)
    assert(mi.tp === 15)
    assert(mi.fp === 9)
    assert(mi.fn === 3)
    assert(mi.tn === 5)

    val m = Measure(tp = mi.tp, fp = mi.fp, fn = mi.fn, tn = mi.tn)
    assert(m.precision === 0.625)
    assert(m.recall === 0.833)
    assert(m.accuracy === 0.625)
    assert(m.fscore === 0.714)

    val ma = MacroAverage(pred, cats)
    assert(ma.tp(A) === 6)
    assert(ma.fp(A) === 2)
    assert(ma.fn(A) === 0)
    assert(ma.tn(A) === 0)
    assert(ma.tp(B) === 0)
    assert(ma.fp(C) === 4)
    assert(ma.tp(D) === 5)
    assert(ma.fp(D) === 3)

    val ex = ExampleBased(pred, cats)
    assert(ex.subsetAcc === 0.125)
    assert(ex.hloss === 0.4375)
    assert(ex.precision === 0.625)
    assert(ex.recall === 0.875)
    assert(ex.accuracy === 0.5625)
  }

  test("feature vector") {
    val a1id = "a1"
    val ann1 = Seq(
      Annotation(articleId = a1id, phrase = A, mc = 1, wd = A),
      Annotation(articleId = a1id, phrase = B, mc = 2, wd = B),
      Annotation(articleId = a1id, phrase = C, mc = 3, wd = C)
    )
    val a1 = Article(id = a1id, ann = ann1.map(x => (x.id, x)).toMap)

    val a2id = "a2"
    val ann2 = Seq(
      Annotation(articleId = a2id, phrase = B, mc = 2, wd = B),
      Annotation(articleId = a2id, phrase = C, mc = 1, wd = C),
      Annotation(articleId = a2id, phrase = D, mc = 1, wd = D)
    )
    val a2 = Article(id = a2id, ann = ann2.map(x => (x.id, x)).toMap)

    val a3id = "a3"
    val ann3 = Seq(
      Annotation(articleId = a3id, phrase = C, mc = 3, wd = C),
      Annotation(articleId = a3id, phrase = D, mc = 2, wd = D)
    )
    val a3 = Article(id = a3id, ann = ann3.map(x => (x.id, x)).toMap)

    val rdd = CorpusContext.sc.parallelize(Seq(a1, a2, a3))
    val tc = TC(rdd)
    assert(tc.documentCount === 3)
    assert(tc.documentsWithTerm(A) === 1)
    assert(tc.documentsWithTerm(B) === 2)
    assert(tc.documentsWithTerm(C) === 3)
    assert(tc.documentsWithTerm(D) === 2)
    assert(tc.maxFrequencyInDocument(a1id) === 3)
    assert(tc.maxFrequencyInDocument(a2id) === 2)
    assert(tc.maxFrequencyInDocument(a3id) === 3)
    assert(tc.frequencySumInDocument(a1id) === 6)
    assert(tc.frequencySumInDocument(a2id) === 4)
    assert(tc.frequencySumInDocument(a3id) === 5)
    //    assert(tc.frequencySumInCorpus(A) === 1)
    //    assert(tc.frequencySumInCorpus(B) === 4)
    //    assert(tc.frequencySumInCorpus(C) === 7)
    //    assert(tc.frequencySumInCorpus(D) === 3)

    val tfidfann1 = ann1(0).copy(tfIdf = tc.tfidf(ann1(0)))
    val tfidfann2 = ann1(1).copy(tfIdf = tc.tfidf(ann1(1)))
    val tfidfann3 = ann1(2).copy(tfIdf = tc.tfidf(ann1(2)))

    val ans1 = (1d / 6d) * 1.098
    val ans2 = (2d / 6d) * 0.405
    val ans3 = 0.0

    assert(tfidfann1.tfIdf === ans1)
    assert(tfidfann2.tfIdf === ans2)
    assert(tfidfann3.tfIdf === ans3)
  }
}
