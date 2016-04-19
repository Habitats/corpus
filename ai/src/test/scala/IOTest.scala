/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus._
import no.habitats.corpus.common.Config
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import util.Samples


@RunWith(classOf[JUnitRunner])
class IOTest extends FunSuite with Samples {

  val log = LoggerFactory.getLogger(getClass)

  test("load articles from disk") {
    assert(articles.size == 4)
    assert(annotations.size == 4)
  }

  test("annotate an article") {
    val a1 = Corpus.toGoogleAnnotated(articles.find(_.id == "1822395").get)
    assert(a1.ann.map(_._2.phrase).toSet.intersect(Set("Sunni", "Baghdad", "Iraqi")).size == 3)
    assert(a1.ann.size == 12)
    assert(a1.ann.map(_._2.id).toSet.intersect(Set("/m/078tg", "/m/01fqm", "/m/0d05q4")).size == 3)
  }

  test("walking") {
    val limit = 3000
    val files = IO.walk(Config.dataPath + "/nyt/", count = limit, filter = "1")
    log.info("Numfiles: " + files.size)
    assert(files.size === limit)
    //    files.map(_.getName).foreach(log.info)
  }

  //  test("binary serialization") {
  //    val originalArticles = articles
  //    // test binary serialization
  //    val start = System.currentTimeMillis
  //    val binaryCacheFile = new File(Config.cachePath + testCache + "_binary.cache")
  //    binaryCacheFile.delete
  //    // serialize
  //    IO.cacheBinary(originalArticles, binaryCacheFile)
  //    println(s"Binary caching: ${System.currentTimeMillis() - start} ms")
  //    // load
  //    val binarySerializedArticles = IO.loadBinary(Source.fromFile(binaryCacheFile)(Codec.ISO8859))
  //    assert(originalArticles == binarySerializedArticles)
  //    binaryCacheFile.delete
  //    println(s"Binary loading: ${System.currentTimeMillis() - start} ms")
  //  }


}
