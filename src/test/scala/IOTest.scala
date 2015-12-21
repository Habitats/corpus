/**
  * Created by Patrick on 13.11.2015.
  */

import java.io.{PrintWriter, File}

import no.habitats.corpus._
import no.habitats.corpus.models.Article
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.io.Source


@RunWith(classOf[JUnitRunner])
class IOTest extends FunSuite {

  val testCache = "test"
  val articles = {
    val raw = Corpus.articles(files = Corpus.walk(new File(Config.testPath + testCache), ".xml"))
    val annotations = Corpus.annotations()
    val articles = Corpus.annotatedArticles(raw, annotations)
    articles.toSeq
  }
  val annotations = articles.flatMap(_.ann)
  val a1: Article = articles.find(_.id == "1822395").get

  test("load articles from disk") {
    assert(articles.size == 4)
    assert(annotations.size == 101)
    assert(a1.ann.size == 12)
    assert(a1.ann.map(_._2.phrase).toSet.intersect(Set("Sunni", "Baghdad", "Iraqi")).size == 3)
    assert(a1.ann.map(_._2.id).toSet.intersect(Set("/m/078tg", "/m/01fqm", "/m/0d05q4")).size == 3)
    assert(a1.ann("/m/078tg").salience == 0d)
    assert(a1.ann("/m/01fqm").salience == 1d)
  }

  test("serializing test") {
    // add the json serializer
    val json = Set[Cache](new JsonSerializer)
    // get the test articles
    val cache = Config.testPath + testCache
    // create actual articles
    val originalArticles = articles
    println(s"Loaded ${originalArticles.size} articles! Starting serialization testing ...")
    json.foreach(j => {
      val start = System.currentTimeMillis
      // convert to json
      val json = j.toJson(originalArticles)
      val s = cache + ".cache"
      val file = new File(s)
      file.delete
      file.createNewFile
      // write to file
      val writer = new PrintWriter(s, "iso-8859-1")
      writer.println(json)
      println(s"${j.name} caching: ${System.currentTimeMillis() - start} ms")
      writer.close()
      val source = Source.fromFile(s, "iso-8859-1")
      // read from file
      val serializedArticles = j.fromJson(source.mkString)
      source.close
      file.delete
      println(s"${j.name} loading: ${System.currentTimeMillis() - start} ms")
      assert(originalArticles == serializedArticles)
    })
  }

  test("binary serialization") {
    val originalArticles = articles
    // test binary serialization
    val start = System.currentTimeMillis
    val binaryCacheFile = new File(Config.cachePath + testCache + "_binary.cache")
    binaryCacheFile.delete
    // serialize
    IO.cacheBinary(originalArticles, binaryCacheFile)
    println(s"Binary caching: ${System.currentTimeMillis() - start} ms")
    // load
    val binarySerializedArticles = IO.loadBinary(Source.fromFile(binaryCacheFile, "iso-8859-1"))
    assert(originalArticles == binarySerializedArticles)
    binaryCacheFile.delete
    println(s"Binary loading: ${System.currentTimeMillis() - start} ms")
  }


}
