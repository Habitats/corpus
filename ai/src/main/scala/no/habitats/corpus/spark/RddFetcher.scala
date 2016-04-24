package no.habitats.corpus.spark

import java.io.File
import java.util.{Collections, Random}

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.models.{Article, DBPediaAnnotation}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException}

import scala.collection.JavaConverters._

object RddFetcher {

  //  lazy val balanced       : RDD[Article] = balanced(Config.category)
  lazy val minimal: Map[String, (Set[String], Set[String])] = Config.dataFile(Config.dataPath + "nyt/minimal.txt").getLines().toSeq.map(l => {
    val id = l.split(" ")(0)
    val iptc = l.split(" ")(1).split(",")
    val fb = l.split(" ")(2).split(",")
    (id, (iptc.toSet, fb.toSet))
  }).toMap

  lazy val test           : RDD[Article] = limit(sc.textFile(Config.testWithAnnotation, Config.partitions).map(JsonSingle.fromSingleJson))
  lazy val rdd            : RDD[Article] = fetchRDD(annotated = false)
  lazy val annotatedRdd   : RDD[Article] = fetchRDD(annotated = true)
  lazy val annotatedW2VRdd: RDD[Article] = {
    val path = Config.nytCorpusW2VAnnotated
    val rdd = sc.textFile(path, Config.partitions)
      .map(JsonSingle.fromSingleJson)
      .filter(_.ann.size > Config.phraseSkipThreshold)
      .filter(_.iptc.nonEmpty)
    limit(rdd)
  }

  def limit(rdd: RDD[Article]): RDD[Article] = if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(Config.count)) else rdd

  def balanced(label: String): RDD[Article] = limit(sc.textFile(Config.balanced(label), Config.partitions).map(JsonSingle.fromSingleJson))

  /** Create a new dataset with all articles with a given label, and the same amount of randomly sampled articles other labels */
  def createBalanced(label: String, all: RDD[Article]): RDD[Article] = {
    val idLabeled: Set[String] = minimal.filter(_._2._1.contains(label)).keySet
    val idOther: Set[String] = {
      val arr = minimal.filter(a => !a._2._1.contains(label)).keySet.toBuffer.asJava
      Collections.shuffle(arr, new Random(Config.seed))
      arr.asScala.take(idLabeled.size).toSet
    }
    // Need to shuffle the examples for training purposes
    all.filter(a => idLabeled.contains(a.id) || idOther.contains(a.id)).sortBy(a => Math.random)
  }

  private def fetchRDD(annotated: Boolean): RDD[Article] = {
    val path = if (annotated) Config.nytCorpusDbpediaAnnotated else Config.nytCorpus
    var rdd = Config.rdd match {
      case "cache" => cachedRdd(sc)
      case "local" => sc.textFile(path, Config.partitions).map(JsonSingle.fromSingleJson)
    }
    rdd = limit(rdd)
    rdd = if (Config.iptcFilter.nonEmpty) rdd.filter(a => a.iptc.intersect(Config.iptcFilter).nonEmpty) else rdd
    rdd
  }

  def dbpedia(sc: SparkContext, name: String = Config.dbpedia): RDD[DBPediaAnnotation] = {
    val rdd = sc.textFile(name).map(DBPediaAnnotation.fromSingleJson)
    if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(Config.count)) else rdd
  }

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
