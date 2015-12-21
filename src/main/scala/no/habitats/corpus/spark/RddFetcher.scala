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
    Preprocess.computeIptc(rdd, Config.broadMatch)
  }

  def localRdd(sc: SparkContext): RDD[Article] = {
    val s = System.currentTimeMillis
    Log.i("Loading local RDD ...")
    val annotations = Corpus.annotations()
    val articles = Corpus.articles()
    val annotatedArticles = Corpus.annotatedArticles(articles, annotations).map(_.strip)
    val rdd = sc.parallelize(annotatedArticles)
    Log.i(s"Loaded ${annotatedArticles.size} articles in ${(System.currentTimeMillis - s) / 1000} seconds")
    cache(rdd)
    rdd
  }

  def cachedRdd(sc: SparkContext): RDD[Article] = {
    val s = System.currentTimeMillis
    val useRdd = new File(IO.rddCacheDir).exists

    Log.i(s"Loading cached RDD from ${
      useRdd match {
        case true => "object file"
        case false => "local cache"
      }
    } ...")
    // ### LOADING
    val rdd = useRdd match {
      case true => IO.loadRdd(sc).repartition(Config.partitions)
      case false => sc.parallelize(IO.load())
    }
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
    val rdd = Config.data match {
      case "test_tiny" => sc.parallelize(hbase.take(1122))
      case "test" => sc.parallelize(hbase.take(9360))
      case "test_medium" => sc.parallelize(hbase.take(26087))
      case "test_big" => hbase.repartition(Config.partitions)
    }
    Log.i("Loading completed in " + ((System.currentTimeMillis - s) / 1000) + " seconds")
    rdd
  }

  def cache(rdd: RDD[Article]) = {
    try {
      if (Config.cluster) {
        IO.cacheRdd(rdd)
      } else {
        IO.cache(rdd.collect)
        IO.cacheRdd(rdd)
      }
    } catch {
      case e: Exception => Log.e("Could not cache RDD!"); Log.e(e)
    }
  }
}
