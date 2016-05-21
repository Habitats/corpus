package no.habitats.corpus

import java.io._
import java.nio.file.{FileSystems, Files}

import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, Log}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

object IO {
  val rddCacheDir = Config.cachePath + "rdd_" + Config.count
  val cacheFile   = Config.cachePath + Config.count + ".cache"

  def walk(path: String, count: Int = 100, filter: String = ""): Seq[File] = {
    val dir = FileSystems.getDefault.getPath(path)
    Log.v("Walking directory ...")
    Files.walk(dir).iterator().asScala.filter(Files.isRegularFile(_)).filter(p => p.toFile.getName.contains(filter)).take(count).map(_.toFile).toSeq
  }

  def copy(f: File) = {
    val src = f
    val dir = new File(Config.dataPath + "relevant/")
    dir.mkdirs
    val dest = new File(dir, f.getName)
    if (!dest.exists) {
      new FileOutputStream(dest) getChannel() transferFrom(new FileInputStream(src) getChannel, 0, Long.MaxValue)
    }
  }

  def cacheAnnotationDistribution(rdd: RDD[Article], ontology: String, iteration: Int) = {
    val annotations = rdd.flatMap(_.ann.values).map(ann => (ann.id, ann.mc)).reduceByKey(_ + _).collect.sortBy(_._2)
    val annotationsUnique = rdd.flatMap(_.ann.values).map(ann => (ann.id, 1)).reduceByKey(_ + _).collect.sortBy(_._2)

    def cache(name: String, annotations: Seq[(String, Int)]) = {
      val f = new File(name)
      val p = new PrintWriter(f)
      f.createNewFile()
      annotations.foreach(ann => p.println(f"${ann._1}%15s ${ann._2}%10d"))
      p.close()
    }

    cache(s"${iteration}annotation_distribution$ontology.txt", annotations)
    cache(s"${iteration}annotation_distribution_unique$ontology.txt", annotationsUnique)
  }

  def cacheRdd(rdd: RDD[Article], cacheDir: String = rddCacheDir) = {
    val dir = new File(cacheDir)
    Log.i(s"Caching rdd to ${dir.getAbsolutePath} ...")
    // rdd cache
    if (dir.exists) {
      dir.listFiles().foreach(f => {
        if (f.isDirectory) f.listFiles().foreach(_.delete)
        f.delete
      })
      dir.delete
    }
    rdd.saveAsObjectFile("file:///" + cacheDir)
  }

  def loadRdd(sc: SparkContext, cacheDir: String = rddCacheDir): RDD[Article] = {
    sc.objectFile[Article]("file:///" + cacheDir)
  }
}

object JsonSingle {
  implicit val formats  = Serialization.formats(NoTypeHints)
  lazy     val jsonFile = new File(Config.dataPath + "nyt_corpus.txt")

  def cacheRawNYTtoJson(count: Int = Config.count, articles: Seq[Article] = Nil) = {
    jsonFile.delete
    jsonFile.createNewFile
    val p = new PrintWriter(jsonFile, "ISO-8859-1")
    (if (articles == Nil) Corpus.articlesFromXML(count = count) else articles)
      .map(Article.serialize)
      .foreach(p.println)
    p.close
  }

  def load(count: Int = -1): Seq[Article] = {
    val source = Source.fromFile(jsonFile)(Codec.ISO8859)
    val articles = source.getLines().take(count).map(f => fromSingleJson(f)).toList
    source.close
    articles
  }

  @Deprecated
  def toSingleJson(article: Article): String = {
    write(article.copy(body = article.body.replace("\n", " ")))
  }

  @Deprecated
  def fromSingleJson(string: String): Article = {
    val a = read[Article](string)
    a.copy(body = Article.safeString(a.body), desc = a.desc.map(Article.safeString), hl = Article.safeString(a.hl)) // remove chars used for string serialization
  }
}

