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

  // Raw NYT Corpus articles without annotations
  lazy val rdd                        : RDD[Article] = fetch("nyt/nyt_corpus.json")
  // Processed NYT Corpus articles with annotations
  lazy val annotatedRdd               : RDD[Article] = fetch("nyt/nyt_corpus_annotated_0.5.json")
  // Filtered NYT Corpus articles
  lazy val nytCorpusW2VAnnotated      : RDD[Article] = fetch("nyt/nyt_corpus_annotated_w2v_0.5_min10.json")
  // Articles filtered before split
  lazy val annotatedTestW2V           : RDD[Article] = fetch("nyt/nyt_test_w2v_5.json", 0.2)
  lazy val annotatedTrainW2V          : RDD[Article] = fetch("nyt/nyt_train_w2v_5.json", 0.6)
  lazy val annotatedValidationW2V     : RDD[Article] = fetch("nyt/nyt_validation_w2v_5.json", 0.2)
  // Articles split in chronological order based on ID
  lazy val annotatedTestOrdered       : RDD[Article] = fetch("nyt/nyt_test_ordered.json", 0.2)
  lazy val annotatedTrainOrdered      : RDD[Article] = fetch("nyt/nyt_train_ordered.json", 0.6)
  lazy val annotatedValidationOrdered : RDD[Article] = fetch("nyt/nyt_validation_ordered.json", 0.2)
  // Articles split randomly
  lazy val annotatedTestShuffled      : RDD[Article] = fetch("nyt/nyt_test_shuffled.json", 0.2)
  lazy val annotatedTrainShuffled     : RDD[Article] = fetch("nyt/nyt_train_shuffled.json", 0.6)
  lazy val annotatedValidationShuffled: RDD[Article] = fetch("nyt/nyt_validation_shuffled.json", 0.2)
  // Articles sub-sampled based on the minimal category
  lazy val subTestW2V                 : RDD[Article] = fetch("nyt/subsampled_test_w2v.json", 0.2)
  lazy val subTrainW2V                : RDD[Article] = fetch("nyt/subsampled_train_w2v.json", 0.6)
  lazy val subValidationW2V           : RDD[Article] = fetch("nyt/subsampled_validation_w2v.json", 0.2)
  lazy val subTestOrdered             : RDD[Article] = fetch("nyt/subsampled_test_ordered.json", 0.2)
  lazy val subTrainOrdered            : RDD[Article] = fetch("nyt/subsampled_train_ordered.json", 0.6)
  lazy val subValidationOrdered       : RDD[Article] = fetch("nyt/subsampled_validation_ordered.json", 0.2)
  lazy val subTestShuffled            : RDD[Article] = fetch("nyt/subsampled_test_shuffled.json", 0.2)
  lazy val subTrainShuffled           : RDD[Article] = fetch("nyt/subsampled_train_shuffled.json", 0.6)
  lazy val subValidationShuffled      : RDD[Article] = fetch("nyt/subsampled_validation_shuffled.json", 0.2)

  def limit(rdd: RDD[Article], fraction: Double = 1): RDD[Article] = {
    val num = (Config.count * fraction).toInt
    if (Config.count < Integer.MAX_VALUE) sc.parallelize(rdd.take(num))
    else rdd
  }

  def fetch(name: String, fraction: Double = 1): RDD[Article] = {
    limit(sc.textFile(Config.dataPath + name, (Config.partitions * fraction).toInt).map(JsonSingle.fromSingleJson), fraction)
  }

  def balanced(label: String, train: Boolean): RDD[Article] = fetch(Config.balanced(label), if (train) 0.6 else 0.2)

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
