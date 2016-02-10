package no.habitats.corpus.spark

import no.habitats.corpus.hbase.HBaseUtil
import no.habitats.corpus.models.Article
import no.habitats.corpus.{Config, Log, Prefs}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object SparkUtil {
  val cacheDir = "cache"
  lazy val sc = Context.sc
  var iter = 0

  def sparkTest() = {
    Log.v(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
  }

  lazy val rdd: RDD[Article] = RddFetcher.rdd(sc)

  def main(args: Array[String]) = {

    // args
    if (args.length > 0) Config.local = args(0).toBoolean
    if (args.length > 1) Config.rdd = args(1)
    if (args.length > 2) Config.job = args(2)
    if (args.length > 3) Config.data = args(3)
    if (args.length > 4) Config.partitions = args(4).toInt

    Log.init()
    Log.r("Starting Corpus job ...")
    val s = System.currentTimeMillis

    val conf = s"Current config -> Data: ${Config.data} - job: ${Config.job} - RDD: ${Config.rdd} - Partitions: ${Config.partitions}"
    Log.r(conf)
    Log.i(f"Loading articles ...")

    Config.job match {
      case "test" => Log.r(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
      case "preprocess" => Preprocess.preprocess(sc, sc.broadcast(Prefs()), rdd)
      case "train" =>
      case "load" =>
        HBaseUtil.init()
        Log.i("Loading HBase ...")
        HBaseUtil.add(rdd.collect)
        Log.i("HBase loading complete!")
      case "count" => Log.r(s"Counting job: ${rdd.count} articles ...")
      case _ => Log.r("No job ... Exiting!")
    }
    Log.r("Job completed in " + ((System.currentTimeMillis - s) / 1000) + " seconds")
    sc.stop
  }

  /////////////////////
  // Utility methods //
  /////////////////////

  def wordCount(rdd: RDD[Article]): Map[String, Int] = {
    rdd.map(_.body).flatMap(_.split("\\s+"))
      .map(w => w.replaceAll("[^a-zA-Z0-9]", ""))
      .map(w => (w, 1)).reduceByKey(_ + _)
      .collect.sortBy(_._2).reverse.toMap
  }

  def memstat() = {
    //Getting the runtime reference from system
    val rt = Runtime.getRuntime
    val mb = 1024 * 1024
    Log.i(s"Used Memory: ${(rt.totalMemory - rt.freeMemory) / mb} - Free Memory: ${rt.freeMemory / mb} - Total Memory: ${rt.totalMemory / mb} - Max Memory: ${rt.maxMemory / mb}")
  }

  def debugPrint(rdd: RDD[Article], numArticles: Int = 1): Unit = {
    Log.v(s"Debug print - Articles: ${rdd.count} - Annotations: ${rdd.flatMap(_.ann).count} ...")
    rdd.take(numArticles).foreach(a => {
      Log.v(a)
      a.ann.values.foreach(Log.v)
      Log.v("")
    })
  }

  def multiLabelTest(rdd: RDD[Article], test: RDD[LabeledPoint], models: Seq[(NaiveBayesModel, String)]): Seq[(String, Double)] = {
    val res = for ((m, l) <- models) yield {
      val predictionAndLabel = test.map(p => (m.predict(p.features), p.label))
      val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
      (l, accuracy)
    }
    res
  }

  ///////////////////////////////////////////////////////
  // helper methods retrieve an RDD -- local or remote //
  ///////////////////////////////////////////////////////

  def stats(rdd: RDD[Article]) = {
    val lines = rdd.count
    val partitions = rdd.partitions.length
    val storageLevel = rdd.getStorageLevel.description
    val sample = rdd.sample(false, Math.min(1.0, 5.0 / lines)).collect
    Log.i(s"ID: ${rdd.id} - Lines: $lines - Partitions: $partitions - Storage Level: $storageLevel")
    sample.foreach(Log.v)
  }
}

