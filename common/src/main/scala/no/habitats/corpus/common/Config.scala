package no.habitats.corpus.common

import java.io.{File, FileNotFoundException, FileReader}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

object Config {

  val seed = 123
  val NONE = "NONE"

  lazy val dbpedia            = dataPath + "dbpedia/dbpedia-all-0.5.json"
  lazy val dbpediaMini25      = dataPath + "dbpedia/dbpedia_mini_json_0.25.json"
  lazy val dbpediaMini50      = dataPath + "dbpedia/dbpedia_mini_json_0.50.json"
  lazy val dbpediaMini75      = dataPath + "dbpedia/dbpedia_mini_json_0.75.json"
  lazy val dbpediaMini100     = dataPath + "dbpedia/dbpedia_mini_json_1.00.json"
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

  def freebaseToWord2VecIDs = dataPath + s"fb_ids_with_w2v.txt"
  def freebaseToWord2Vec(confidence: Double) = {
    dataPath + s"w2v/fb_w2v_$confidence.txt"
    //    s"r:/fb_w2v_$confidence.txt"
  }
  def documentVectors(confidence: Double) = {
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

  private var args        : Arguments = null
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
  val testPath            : String      = formatPath(conf.getProperty("test_path"))
  val dataPath            : String      = formatPath(conf.getProperty("data_path"))
  val cachePath           : String      = formatPath(conf.getProperty("cache_path"))
  val modelPath           : String      = formatPath(conf.getProperty("model_path"))
  val broadMatch          : Boolean     = conf.getProperty("broad_match").toBoolean
  val wikiDataOnly        : Boolean     = conf.getProperty("wikidata_only").toBoolean
  val wikiDataIncludeBroad: Boolean     = conf.getProperty("wikidata_include_broad").toBoolean
  val phraseSkipThreshold : Int         = conf.getProperty("term_frequency_threshold").toInt
  val minimumAnnotations  : Int         = conf.getProperty("minimum_annotations").toInt
  val iptcFilter          : Set[String] = Some(conf.getProperty("iptc_filter")).map(e => if (e.length > 0) e.split(",").toSet else Set[String]()).get
  val dbpediaSpotlightURL : String      = conf.getProperty("dbpedia_spotlight_url")

  // Dynamic variables for file caching
  // TODO: this shouldn't be here. really.
  var resultsFileName     = "results.txt"
  var resultsCatsFileName = "results_cats.txt"

  // args
  def rdd = args.rdd.getOrElse(conf.getProperty("rdd"))
  def partitions = args.partitions.getOrElse(conf.getProperty("partitions").toInt)
  def count: Int = args.count.getOrElse(conf.getProperty("count").toInt) match {
    case i => if (i == -1) Integer.MAX_VALUE else i
  }

  def setArgs(arr: Array[String]) = {
    lazy val props: Map[String, String] = arr.map(_.split("=") match { case Array(k, v) => k -> v }).toMap
    args = Arguments(
      partitions = props.get("partitions").map(_.toInt),
      rdd = props.get("rdd"),
      job = props.get("job"),
      iptcFilter = props.get("iptcFilter").map(_.split(",").toSet),
      category = props.get("category"),
      count = props.get("count").map(_.toInt),
      spark = props.get("spark").map(_.toBoolean),
      useApi = props.get("useApi").map(_.toBoolean),
      learningRate = props.get("lr").map(_.toDouble),
      l2 = props.get("l2").map(_.toDouble),
      miniBatchSize = props.get("mbs").map(_.toInt),
      cache = props.get("cache").map(_.toBoolean),
      histogram = props.get("histogram").map(_.toBoolean),
      hidden1 = props.get("h1").map(_.toInt),
      hidden2 = props.get("h2").map(_.toInt),
      hidden3 = props.get("h3").map(_.toInt),
      tft = props.get("tft").map(_.toInt),
      superSample = props.get("super").map(_.toBoolean),
      iterations = props.get("iter").map(_.toInt)
    )

    Log.v("ARGUMENTS: " + props.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t", "\n\t", ""))
    Log.v("Categories: " + cats.mkString(", "))
    Log.v("CORPUS CONFIG: " + corpusConfig + "\n\t" + conf.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
    if (local) Log.v("SPARK CONFIG: " + sparkConfig + "\n\t" + sparkProps.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
  }

  lazy val job                   : String          = args.job.getOrElse(conf.getProperty("job"))
  lazy val cache                 : Boolean         = args.cache.getOrElse(true)
  lazy val histogram             : Boolean         = args.histogram.getOrElse(false)
  lazy val useApi                : Boolean         = args.useApi.getOrElse(false)
  lazy val spark                 : Boolean         = args.spark.getOrElse(false)
  lazy val category              : Option[String]  = args.category
  lazy val learningRate          : Option[Double]  = args.learningRate
  lazy val miniBatchSize         : Option[Int]     = args.miniBatchSize
  lazy val iterations            : Option[Int]     = args.iterations
  lazy val hidden1               : Option[Int]     = args.hidden1
  lazy val hidden2               : Option[Int]     = args.hidden2
  lazy val hidden3               : Option[Int]     = args.hidden3
  lazy val superSample           : Option[Boolean] = args.superSample
  lazy val l2                    : Option[Double]  = args.l2
  lazy val termFrequencyThreshold: Option[Int]     = args.tft

  case class Arguments(
                        partitions: Option[Int],
                        rdd: Option[String],
                        job: Option[String],
                        count: Option[Int],
                        category: Option[String],
                        iptcFilter: Option[Set[String]],
                        spark: Option[Boolean],
                        useApi: Option[Boolean],
                        learningRate: Option[Double],
                        l2: Option[Double],
                        miniBatchSize: Option[Int],
                        cache: Option[Boolean],
                        histogram: Option[Boolean],
                        hidden1: Option[Int],
                        hidden2: Option[Int],
                        hidden3: Option[Int],
                        tft: Option[Int],
                        iterations: Option[Int],
                        superSample: Option[Boolean]
                      )
}
