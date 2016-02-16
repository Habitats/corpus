package no.habitats.corpus.spark

import no.habitats.corpus._
import no.habitats.corpus.models.Article
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
    Config.setArgs(args)

    Log.init()
    Log.r("Starting Corpus job ...")
    val s = System.currentTimeMillis

    Log.i(f"Loading articles ...")

    Config.job match {
      case "test" => Log.r(s"Running simple test job ... ${sc.parallelize(1 to 1000).count}")
      case "preprocess" => Preprocess.preprocess(sc, sc.broadcast(Prefs()), rdd)
      case "cacheNYT" => JsonSingle.cache(Config.count)
      case "loadNYT" => Log.v(f"Loaded ${JsonSingle.load(Config.count).size} articles")
      case "print" => printArticles(Config.count)
      case "stats" => stats(rdd)
      case "iptcDistribution" => calculateIPTCDistribution(Config.count)
      case "count" => Log.r(s"Counting job: ${rdd.count} articles ...")
      case _ => Log.r("No job ... Exiting!")
    }
    Log.r("Job completed in " + ((System.currentTimeMillis - s) / 1000) + " seconds")
    sc.stop
  }

  def printArticles(count: Int) = {
    val rddNYT = sc.parallelize(IO.walk(Config.dataPath + "/nyt/", count = count, filter = ".xml"))
      .map(Corpus.toNYT)
    Log.v("FIRST SIZE: " + rddNYT.count)
    Log.v("Article: " + Article("asd"))
    Log.v(rddNYT.collect().map(_.toString).mkString(f"SIMPLE PRINT (${rddNYT.count} articles)\n", "\n", ""))
    val rdd = rddNYT.map(Corpus.toArticle)
    Log.v(rdd.collect().map(_.toString).mkString(f"SIMPLE PRINT (${rdd.count} articles)\n", "\n", ""))
  }

  def calculateIPTCDistribution(count: Int) = {
    val rdd = RddFetcher.rdd(sc)
      .flatMap(_.iptc.toSeq)
      .map(c => (c, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)
    Log.v(rdd.collect.map(c => f"${c._2}%-10s - ${c._1}").mkString("IPTC CATEGORY DISTRIBUTION\n", "\n", ""))
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

  //////////
  // DL4J //
  //////////

  def dl4j = {
    //    val conf = setSetup.getConf()
    //    val trainLayer = new SparkDl4jMultiLayer(sc, conf)
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

