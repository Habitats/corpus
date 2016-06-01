package no.habitats.corpus.common

import java.io.{File, FileNotFoundException, FileReader}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

object Config {

  val seed = 123
  val NONE = "NONE"

  lazy val combinedIds        = dataPath + "dbpedia/combined_ids_0.5.txt"
  lazy val freebaseToWikidata = dataPath + "dbpedia/fb_to_wd_all.txt"
  lazy val wikidataToFreebase = dataPath + "dbpedia/wd_to_fb.txt"
  lazy val wikidataToDbPedia  = dataPath + "dbpedia/wikidata_to_dbpedia.txt"

  lazy val cats: Seq[String] = Config.category match {
    case Some(s) if s.startsWith("@") => {
      val startCat: String = s.substring(1, s.length)
      val all: Seq[String] = IPTC.topCategories
      all.takeRight(all.size - all.indexOf(startCat))
    }
    case Some(s) => Seq(s)
    case None => IPTC.topCategories
  }

  lazy val start = System.currentTimeMillis()
  def init() = start

  def freebaseToWord2VecIDs = dataPath + s"fb_ids_with_w2v.txt"
  def freebaseToWord2Vec(confidence: Double = Config.confidence.getOrElse(0.5)) = {
    dataPath + s"w2v/fb_w2v_$confidence.txt"
    //    s"r:/fb_w2v_$confidence.txt"
  }
  def documentVectors(confidence: Double = Config.confidence.getOrElse(0.5)) = {
    dataPath + s"w2v/document_vectors_$confidence.txt"
    //    s"r:/document_vectors_$confidence.json"
  }

  def local: Boolean = System.getProperty("os.name").startsWith("Windows")

  def dataFile(s: String): BufferedSource = Try(Source.fromFile(s)(Codec.ISO8859)) match {
    case Success(file) => Log.v("Loading data file: " + s); file
    case Failure(ex) => throw new FileNotFoundException(s"Error loading data file: $s - ERROR: ${ex.getMessage}")
  }

  def testFile(s: String): BufferedSource = Try(Source.fromFile(testPath + s)(Codec.ISO8859)) match {
    case Success(file) => Log.v("Loading test file: " + s); file
    case Failure(ex) => throw new FileNotFoundException(s"Error loading test file: $s - ERROR: ${ex.getMessage}")
  }

  lazy val sparkProps = {
    Log.v("NO SPARK CONFIG, USING DEFAULT. THIS SHOULD ONLY HAPPEN DURING TESTING.")
    sparkConfig = configRoot + "spark_local.properties"
    val conf = new Properties
    val propsFile = Try(Source.fromInputStream(getClass.getResourceAsStream(sparkConfig))(Codec.UTF8).bufferedReader()).getOrElse(new FileReader(new File(sparkConfig)))
    conf.load(propsFile)
    conf
  }

  lazy val conf = {
    Log.v("NO CORPUS CONFIG, USING DEFAULT. THIS SHOULD ONLY HAPPEN DURING TESTING.")
    val present = new File(corpusConfig).exists()
    Log.v(s"Loading config (present: $present): " + corpusConfig)
    val conf = new Properties
    val propsFile = Try(Source.fromInputStream(getClass.getResourceAsStream(corpusConfig))(Codec.UTF8).bufferedReader()).getOrElse(new FileReader(new File(corpusConfig)))
    conf.load(propsFile)
    conf
  }

  def formatPath(path: String): String = {
    if (local) {
      path
        .replace("%ARCHIVE_HOME%", sys.env("ARCHIVE_HOME"))
        .replace("%DROPBOX_HOME%", sys.env("DROPBOX_HOME"))
        .replace("\\", "/")
    } else {
      path
        .replace("~", System.getProperty("user.home"))
        .replace("\\", "/")
    }
  }

  def balanced(label: String): String = dataPath + s"nyt/separated_w2v_min10/${label}_balanced.txt"

  private var args        : Arguments = Arguments()
  private var sparkConfig : String    = null
  private val configRoot  : String    = {
    val c = if (local) {
      "%DROPBOX_HOME%/code/projects/corpus/common/src/main/resources/"
    } else {
      "~/corpus/"
    }
    formatPath(c)
  }
  private val corpusConfig: String    = if (local) configRoot + "corpus_local.properties" else "/corpus_cluster.properties"

  // static
  val testPath            : String  = formatPath(conf.getProperty("test_path"))
  val dataPath            : String  = formatPath(conf.getProperty("data_path"))
  val cachePath           : String  = formatPath(conf.getProperty("cache_path"))
  val modelPath           : String  = formatPath(conf.getProperty("model_path"))
  val broadMatch          : Boolean = conf.getProperty("broad_match").toBoolean
  val wikiDataOnly        : Boolean = conf.getProperty("wikidata_only").toBoolean
  val wikiDataIncludeBroad: Boolean = conf.getProperty("wikidata_include_broad").toBoolean
  val dbpediaSpotlightURL : String  = conf.getProperty("dbpedia_spotlight_url")

  def getArgs: Arguments = args
  def prefsName: String = args.copy(job = None).toString.replaceAll("\\s+|,", "_")

  def setArgs(arr: Array[String]) = {
    lazy val props: mutable.Map[String, String] = mutable.Map() ++ arr.map(_.split("=") match { case Array(k, v) => k -> v }).toMap
    val m: String = "ARGUMENTS: " + props.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t", "\n\t", "")
    Log.v(m)
    args = Arguments(
      partitions = props.remove("partitions").map(_.toInt),
      parallelism = props.remove("parallelism").map(_.toInt),
      rdd = props.remove("rdd"),
      job = props.remove("job"),
      iptcFilter = props.remove("iptcFilter").map(_.split(",").toSet),
      category = props.remove("category"),
      types = props.remove("types").map(_.toBoolean),
      count = props.remove("count").map(_.toInt),
      useApi = props.remove("useApi").map(_.toBoolean),
      learningRate = props.remove("lr").map(_.toDouble),
      confidence = props.remove("confidence").map(_.toDouble),
      l2 = props.remove("l2").map(_.toDouble),
      miniBatchSize = props.remove("mbs").map(_.toInt),
      cache = props.remove("cache").map(_.toBoolean),
      histogram = props.remove("histogram").map(_.toBoolean),
      hidden1 = props.remove("h1").map(_.toInt),
      hidden2 = props.remove("h2").map(_.toInt),
      hidden3 = props.remove("h3").map(_.toInt),
      tft = props.remove("tft").map(_.toInt),
      superSample = props.remove("super").map(_.toBoolean),
      memo = props.remove("memo").map(_.toBoolean),
      iterations = props.remove("iter").map(_.toInt),
      tag = props.remove("tag"),
      epoch = props.remove("epoch").map(_.toInt),
      logResults = props.remove("logres").map(_.toBoolean)
    )
    if (props.nonEmpty) {Log.v("Illegal props: " + props.mkString(", ")); System.exit(0)}
    Log.v("Categories: " + cats.mkString(", "))
    Log.v("CORPUS CONFIG: " + corpusConfig + "\n\t" + conf.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
    if (local) Log.v("SPARK CONFIG: " + sparkConfig + "\n\t" + sparkProps.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
  }

  lazy val job                   : String          = args.job.getOrElse(conf.getProperty("job"))
  lazy val rdd                   : String          = args.rdd.getOrElse(conf.getProperty("rdd"))
  lazy val partitions            : Int             = Try(args.partitions.getOrElse(conf.getProperty("partitions").toInt)).getOrElse(20)
  lazy val parallelism           : Int             = args.parallelism.getOrElse(1)
  lazy val count                 : Int             = Try(args.count.getOrElse(conf.getProperty("count").toInt) match { case i => if (i == -1) Integer.MAX_VALUE else i }).getOrElse(Integer.MAX_VALUE)
  lazy val cache                 : Boolean         = args.cache.getOrElse(false)
  lazy val histogram             : Boolean         = args.histogram.getOrElse(false)
  lazy val useApi                : Boolean         = args.useApi.getOrElse(false)
  lazy val memo                  : Boolean         = args.memo.getOrElse(false)
  lazy val category              : Option[String]  = args.category
  lazy val tag                   : Option[String]  = args.tag
  lazy val learningRate          : Option[Double]  = args.learningRate
  lazy val confidence            : Option[Double]  = args.confidence
  lazy val miniBatchSize         : Option[Int]     = args.miniBatchSize
  lazy val epoch                 : Option[Int]     = args.epoch
  lazy val iterations            : Option[Int]     = args.iterations
  lazy val hidden1               : Option[Int]     = args.hidden1
  lazy val hidden2               : Option[Int]     = args.hidden2
  lazy val hidden3               : Option[Int]     = args.hidden3
  lazy val superSample           : Option[Boolean] = args.superSample
  lazy val types                 : Option[Boolean] = args.types
  lazy val l2                    : Option[Double]  = args.l2
  lazy val termFrequencyThreshold: Option[Int]     = args.tft
  lazy val logResults            : Option[Boolean] = args.logResults

  case class Arguments(
                        partitions: Option[Int] = None,
                        parallelism: Option[Int] = None,
                        rdd: Option[String] = None,
                        job: Option[String] = None,
                        count: Option[Int] = None,
                        category: Option[String] = None,
                        tag: Option[String] = None,
                        iptcFilter: Option[Set[String]] = None,
                        useApi: Option[Boolean] = None,
                        learningRate: Option[Double] = None,
                        confidence: Option[Double] = None,
                        l2: Option[Double] = None,
                        miniBatchSize: Option[Int] = None,
                        cache: Option[Boolean] = None,
                        histogram: Option[Boolean] = None,
                        logResults: Option[Boolean] = None,
                        types: Option[Boolean] = None,
                        hidden1: Option[Int] = None,
                        hidden2: Option[Int] = None,
                        hidden3: Option[Int] = None,
                        tft: Option[Int] = None,
                        iterations: Option[Int] = None,
                        epoch: Option[Int] = None,
                        memo: Option[Boolean] = None,
                        superSample: Option[Boolean] = None
                      ) {
    override def toString = {
      import java.lang.reflect._
      getClass.getDeclaredFields.flatMap { field: Field =>
        field.setAccessible(true)
        val value: Option[_] = field.get(this).asInstanceOf[Option[_]]
        value.map(v => field.getName + " = " + v)
      }.sorted.mkString(", ")
    }
  }
}
