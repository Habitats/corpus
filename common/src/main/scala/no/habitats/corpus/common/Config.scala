package no.habitats.corpus.common

import java.io.{File, FileNotFoundException, FileReader}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Codec, Source}
import scala.util.{Failure, Success, Try}

object Config {

  val seed = 123
  val NONE = "NONE"

  lazy val freebaseToWord2Vec          = dataPath + "nyt/fb_w2v_0.5.txt"
  lazy val freebaseToWord2VecIDs       = dataPath + "nyt/fb_w2v_0.5_ids.txt"
  lazy val dbpedia                     = dataPath + "nyt/dbpedia-all-0.5.json"
  lazy val combinedIds                 = dataPath + "nyt/combined_ids_0.5.txt"
  lazy val freebaseToWikidata          = dataPath + "wikidata/fb_to_wd_all.txt"
  lazy val wikidataToFreebase          = dataPath + "wikidata/wd_to_fb.txt"
  lazy val wikidataToDbPedia           = dataPath + "wikidata/wikidata_to_dbpedia.txt"

  lazy val cats: Seq[String] = Try(Seq(Config.category)).getOrElse(IPTC.topCategories)

  def balanced(label: String): String = dataPath + s"nyt/separated_w2v_min10/${label}_balanced.json"

  private var args           : Arguments = Arguments()
  private var sparkConfig    : String    = null
  private var corpusConfig   : String    = null
  private val localConfigRoot: String    = sys.env("DROPBOX_HOME") + "/code/projects/corpus/ai/src/main/resources/"

  val corpusApiURL = "http://corpus.habitats.no:8090/vec"

  def setArgs(arr: Array[String]) = {
    lazy val props: Map[String, String] = arr.map(_.split("=") match { case Array(k, v) => k -> v }).toMap
    args = Arguments(
      partitions = props.get("partitions").map(_.toInt),
      rdd = props.get("rdd"),
      job = props.get("job"),
      iptcFilter = props.get("iptcFilter").map(_.split(",").toSet),
      category = props.get("category"),
      count = props.get("count").map(_.toInt),
      useApi = props.get("useApi").map(_.toBoolean)
    )

    Log.v("ARGUMENTS: " + props.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t", "\n\t", ""))
    Log.v("CORPUS CONFIG: " + corpusConfig + "\n\t" + conf.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
    Log.v("SPARK CONFIG: " + sparkConfig + "\n\t" + sparkProps.asScala.toSeq.sortBy(_._1).map { case (k, v) => k + " -> " + v }.mkString("\n\t"))
  }

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
    sparkConfig = localConfigRoot + "spark_local.properties"

    val conf = new Properties
    val propsFile = Try(Source.fromInputStream(getClass.getResourceAsStream(sparkConfig))(Codec.UTF8).bufferedReader()).getOrElse(new FileReader(new File(sparkConfig)))
    conf.load(propsFile)
    conf
  }

  lazy val conf = {
    Log.v("NO CORPUS CONFIG, USING DEFAULT. THIS SHOULD ONLY HAPPEN DURING TESTING.")
    corpusConfig = if (System.getProperty("os.name").startsWith("Windows")) {
      localConfigRoot + "corpus_local.properties"
    } else {
      "/corpus_cluster.properties"
    }
    val present = new File(corpusConfig).exists()
    Log.v(s"Loading config (present: $present): " + corpusConfig)
    val conf = new Properties
    val propsFile = Try(Source.fromInputStream(getClass.getResourceAsStream(corpusConfig))(Codec.UTF8).bufferedReader()).getOrElse(new FileReader(new File(corpusConfig)))
    conf.load(propsFile)
    conf
  }

  def formatPath(path: String): String = {
    path
      .replace("~", System.getProperty("user.home"))
      .replace("%ARCHIVE_HOME%", sys.env("ARCHIVE_HOME"))
      .replace("%DROPBOX_HOME%", sys.env("DROPBOX_HOME"))
      .replace("\\", "/")
  }

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
  def job = args.job.getOrElse(conf.getProperty("job"))
  def category = args.category.getOrElse(throw new IllegalArgumentException("NO CATEGORY DEFINED"))
  def useApi = args.useApi.getOrElse(false)

  case class Arguments(
                        partitions: Option[Int] = None,
                        rdd: Option[String] = None,
                        job: Option[String] = None,
                        count: Option[Int] = None,
                        category: Option[String] = None,
                        iptcFilter: Option[Set[String]] = None,
                        useApi: Option[Boolean] = None
                      )
}
