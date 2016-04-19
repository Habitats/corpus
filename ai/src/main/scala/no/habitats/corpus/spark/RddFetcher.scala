package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.models.{Article, DBPediaAnnotation}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException}

object RddFetcher {

  lazy val rdd         : RDD[Article] = fetchRDD(annotated = false)
  lazy val annotatedRdd: RDD[Article] = fetchRDD(annotated = true)

  private def fetchRDD(annotated: Boolean): RDD[Article] = {
    val path = if (annotated) Config.nytCorpusAnnotated else Config.nytCorpus
    var rdd = Config.rdd match {
      case "cache" => cachedRdd(sc)
      case "local" => sc.textFile(path, Config.partitions).map(JsonSingle.fromSingleJson)
    }
    rdd = if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(Config.count)) else rdd
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
