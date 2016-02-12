package no.habitats.corpus.spark

import java.io.File

import no.habitats.corpus._
import no.habitats.corpus.hbase.HBaseUtil
import no.habitats.corpus.models.Article
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkContext, SparkException}

object RddFetcher {

  def rdd(sc: SparkContext): RDD[Article] = {
    val rdd = Config.rdd match {
      case "hbase" => hbaseRdd(sc)
      case "cache" => cachedRdd(sc)
      case "local" => localRdd(sc)
    }
    rdd.cache()
  }

  def localRdd(sc: SparkContext): RDD[Article] = {
    val s = System.currentTimeMillis
    Log.i("Loading local RDD ...")
    val articles = Corpus.articles()
    val annotatedArticles = Corpus.annotatedArticles(articles)
    val rdd = sc.parallelize(annotatedArticles)
    Log.i(s"Loaded ${annotatedArticles.size} articles in ${(System.currentTimeMillis - s) / 1000} seconds")
    cache(rdd)
    rdd
  }

  def pipeline(sc: SparkContext, count: Int): RDD[Article] = {
    val s = System.currentTimeMillis
    Log.i("Loading pipelined RDD ...")
    sc.parallelize(IO.walk(Config.dataPath + "/nyt/", count = count, filter = ".xml"))
      .map(Corpus.toNYT)
      .map(Corpus.toArticle)
      .map(Corpus.toAnnotated)
      .map(Corpus.toIPTC)
  }

  def cachedRdd(sc: SparkContext): RDD[Article] = {
    val s = System.currentTimeMillis
    val useRdd = new File(IO.rddCacheDir).exists

    Log.i(s"Loading cached RDD from ${if (useRdd) "object file" else "local cache"} ...")
    // ### LOADING
    val rdd = if (useRdd) IO.loadRdd(sc).repartition(Config.partitions) else sc.parallelize(IO.load)
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

  def hbaseRdd(sc: SparkContext): RDD[Article] = {
    val s = System.currentTimeMillis
    Log.i("Loading HBase RDD ...")
    val hbase = new NewHadoopRDD(sc, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result], HBaseUtil.conf)
      .map(a => HBaseUtil.toArticle(a._2))
      .filter(_.iptc.nonEmpty)
      .take(Config.count)
    Log.i("Loading completed in " + ((System.currentTimeMillis - s) / 1000) + " seconds")
    sc.parallelize(hbase)
  }

  def cache(rdd: RDD[Article]) = {
    try {
      if (Config.local) {
        IO.cacheRdd(rdd)
      } else {
        IO.cache(rdd.collect)
        IO.cacheRdd(rdd)
      }
    }
    catch {
      case e: Exception => Log.e("Could not cache RDD!"); Log.e(e)
    }
  }
}
