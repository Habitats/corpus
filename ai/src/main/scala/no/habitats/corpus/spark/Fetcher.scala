package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Config, Log}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException}

import scala.collection.Map

object Fetcher {

  //  lazy val balanced       : RDD[Article] = balanced(Config.category)
  lazy val minimal: Map[String, (Set[String], Set[String])] = Config.dataFile(Config.dataPath + "nyt/minimal.txt").getLines().toSeq.map(l => {
    val id = l.split(" ")(0)
    val iptc = l.split(" ")(1).split(",")
    val fb = l.split(" ")(2).split(",")
    (id, (iptc.toSet, fb.toSet))
  }).toMap

  lazy val annotatedTrainW2V     : RDD[Article] = limit(sc.textFile(Config.annotatedTrainW2V, (Config.partitions * 0.6).toInt).map(JsonSingle.fromSingleJson), 0.6)
  lazy val annotatedValidationW2V: RDD[Article] = limit(sc.textFile(Config.annotatedValidationW2V, (Config.partitions * 0.2).toInt).map(JsonSingle.fromSingleJson), 0.2)
  lazy val annotatedTestW2V      : RDD[Article] = limit(sc.textFile(Config.annotatedTestW2V, (Config.partitions * 0.2).toInt).map(JsonSingle.fromSingleJson), 0.2)
  lazy val subTrainW2V           : RDD[Article] = limit(sc.textFile(Config.subTrainW2V, (Config.partitions * 0.6).toInt).map(JsonSingle.fromSingleJson), 0.6)
  lazy val subValidationW2V      : RDD[Article] = limit(sc.textFile(Config.subValidationW2V, (Config.partitions * 0.2).toInt).map(JsonSingle.fromSingleJson), 0.2)
  lazy val subTestW2V            : RDD[Article] = limit(sc.textFile(Config.subTestW2V, (Config.partitions * 0.2).toInt).map(JsonSingle.fromSingleJson), 0.2)
  lazy val rdd                   : RDD[Article] = limit(sc.textFile(Config.nytCorpus, Config.partitions).map(JsonSingle.fromSingleJson))
  lazy val annotatedRdd          : RDD[Article] = limit(sc.textFile(Config.nytCorpusDbpediaAnnotated, Config.partitions).map(JsonSingle.fromSingleJson))

  def limit(rdd: RDD[Article], fraction: Double = 1): RDD[Article] = {
    val num = (Config.count * fraction).toInt
    if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(num))
    else rdd
  }

  def balanced(label: String, train: Boolean): RDD[Article] = limit(sc.textFile(Config.balanced(label), Config.partitions).map(JsonSingle.fromSingleJson), if (train) 0.6 else 0.2)

  def localRdd(sc: SparkContext): RDD[Article] = {
    sc.textFile(Config.nytCorpus, Config.partitions).map(JsonSingle.fromSingleJson)
  }

  def cachedRdd(sc: SparkContext): RDD[Article] = {
    val s = System.currentTimeMillis
    val useRdd = new File(IO.rddCacheDir).exists

    Log.i(s"Loading cached RDD from ${if (useRdd) "object file" else "local cache"} ...")
    // ### LOADING
    val rdd = IO.loadRdd(sc).repartition(Config.partitions)
    Log.i(s"Loading completed in ${(System.currentTimeMillis - s) / 1000} seconds")
    try {
      Log.v("Checking integrity ...")
      Log.v(s"Checked ${rdd.take(10).map(_.copy()).size} articles!")
      rdd
    } catch {
      case e: SparkException =>
        Log.v("Invalid class: " + e.getMessage)
        throw e
    }
  }
}
