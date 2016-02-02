package no.habitats.corpus.spark

import no.habitats.corpus.{Config, Log}
import org.apache.spark.{SparkConf, SparkContext}

object Context {
  lazy val clusterContext = {
    Log.i("Attempting to use cluster context ...")
    Config.standalone = true
    val conf = new SparkConf().setAppName("Corpus")
    val sc = conf.getOption("spark.master") match {
      case Some(_) => new SparkContext(conf)
      case _ => localContext
    }
    sc.setLogLevel("ERROR")
    sc
  }

  lazy val localContext = {
    Log.i("Using local context!")
    Config.standalone = false
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val conf = new SparkConf().setMaster("local[8]").setAppName("Corpus")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc
  }
}
