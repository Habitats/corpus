package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.models.Article
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException}

object RddFetcher {

  var size: Double = 1855658L

  def rdd(sc: SparkContext): RDD[Article] = {
    Config.rdd match {
      case "cache" => cachedRdd(sc)
      case "local" => localRdd(sc)
    }
  }

  def localRdd(sc: SparkContext): RDD[Article] = {
    val rdd = sc.textFile("file:///" + JsonSingle.jsonFile.getAbsolutePath, Config.partitions).map(JsonSingle.fromSingleJson)
    if (Config.count == Integer.MAX_VALUE) rdd else sc.parallelize(rdd.take(Config.count))
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
        Log.v("Falling back to local ...")
        localRdd(sc)
    }
  }

  def cache(rdd: RDD[Article]) = {
    try {
      if (Config.local) {
        IO.cacheRdd(rdd)
      }
      //      else {
      //        IO.cache(rdd.collect)
      //        IO.cacheRdd(rdd)
      //      }
    } catch {
      case e: Exception => Log.e("Could not cache RDD!"); Log.e(e.getMessage)
    }
  }
}
