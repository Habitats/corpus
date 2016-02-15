package no.habitats.corpus

import java.io._
import java.nio.file.{FileSystems, Files}

import no.habitats.corpus.models.Article
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

object IO extends JsonSerializer {
  val rddCacheDir = Config.cachePath + "rdd_" + Config.count
  val cacheFile = Config.cachePath + Config.count + ".cache"

  def walk(path: String, count: Int = 100, filter: String = ""): Seq[File] = {
    val dir = FileSystems.getDefault.getPath(path)
    Log.v("Walking directory ...")
    Files.walk(dir).iterator().asScala.filter(Files.isRegularFile(_)).filter(p => p.toFile.getName.contains(filter)).take(count).map(_.toFile).toSeq
  }

  // General methods
  def cache(seq: Seq[Article], cacheFile: String = cacheFile) = {
    val f = new File(cacheFile)
    Log.i(s"Caching to ${f.getAbsolutePath} ...")
    if (f.exists) {
      f.delete
    }
    f.createNewFile
    cacheJson(seq, f)
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

  def load: Seq[Article] = {
    val f = new File(cacheFile)
    Log.i(f"Using cached file: $f")
    loadJson(Source.fromFile(f, "iso-8859-1"))
  }

  // Json cache
  def cacheJson(seq: Seq[Article], cache: File) = {
    val writer = new PrintWriter(cache)
    val json = toJson(seq)
    writer.print(json)
    writer.close()
  }

  def loadJson(source: Source): Seq[Article] = {
    // do not store cache if cluster
    val lines = try source.mkString finally source.close
    val articles = fromJson(lines)
    Log.i("Parsing complete!")
    articles
  }
}

object JsonSingle {

  import org.json4s._
  import org.json4s.jackson.Serialization
  import org.json4s.jackson.Serialization._

  implicit val formats = Serialization.formats(NoTypeHints)

  def cache(count: Int) = {
    val f = new File(Config.dataPath + "/nyt/nyt_corpus.json")
    f.delete
    f.createNewFile
    val p = new PrintWriter(f, "ISO-8859-1")
    IO.walk(Config.dataPath + "/nyt/", count = Config.count, filter = ".xml")
      .map(Corpus.toNYT)
      .map(Corpus.toArticle)
      .map(Corpus.toIPTC)
      .map(JsonSingle.toSingleJson)
      .foreach(p.println)
    p.close
  }

  def load(count: Int = -1): Seq[Article] = {
    val f = new File(Config.dataPath + "/nyt/nyt_corpus.json")
    val source = Source.fromFile(f)(Codec.ISO8859)
    val articles = source.getLines().take(count).map(f => fromSingleJson(f)).toList
    source.close
    articles
  }

  def toSingleJson(article: Article): String = {
    write(article)
  }

  def fromSingleJson(string: String): Article = {
    read[Article](string)
  }
}

class JsonSerializer extends Cache {

  case class JsonWrapper(articles: Seq[Article])

  import org.json4s._
  import org.json4s.jackson.Serialization
  import org.json4s.jackson.Serialization._

  implicit val formats = Serialization.formats(NoTypeHints)

  override def toJson(articles: Seq[Article]): String = if (articles.size < 100) writePretty(articles) else write(articles)
  override def fromJson(a: String): Seq[Article] = read[Seq[Article]](a)
  override def name: String = "LiftJson"
}

trait Cache {
  def toJson(a: Seq[Article]): String
  def fromJson(json: String): Seq[Article]
  def name: String
}
