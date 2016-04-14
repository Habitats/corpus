package no.habitats.corpus.spark

import no.habitats.corpus.Prefs
import no.habitats.corpus.common.{Config, Log}
import no.habitats.corpus.models.Article
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Experiments {
  var iter = 0

  def baseline(sc: SparkContext, rdd: RDD[Article]) = {
    var iter = 0
    Config.resultsFileName = "res_baseline"
    Config.resultsCatsFileName = "res_baseline_cats"
    val prefs = sc.broadcast(Prefs())
    ML.multiLabelClassification(prefs, Preprocess.preprocess(prefs, rdd))
    iter += 1
  }

  def stats(sc: SparkContext, rdd: RDD[Article]) = {
    val a = Preprocess.preprocess(sc.broadcast(Prefs()), rdd)
    val prefs = sc.broadcast(Prefs(iteration = iter))
    val annCounts = a.flatMap(a => a.iptc.map(c => (c, a.ann.size))).reduceByKey(_ + _).collectAsMap
    Log.toFile(annCounts.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "stats/annotation_pr_iptc.txt")
    val artByAnn = a.flatMap(a => a.iptc.map(c => (c, 1))).reduceByKey(_ + _).collectAsMap
    Log.toFile(artByAnn.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "stats/articles_pr_iptc.txt")
    val iptc = a.flatMap(_.iptc).distinct.collect
    val avgAnnIptc = iptc.map(c => (c, annCounts(c).toDouble / artByAnn(c))).toMap
    Log.toFile(avgAnnIptc.map(c => f"${c._1}%30s ${c._2}%10.0f").mkString("\n"), "stats/average_ann_pr_iptc.txt")
  }

  def frequencyExperiment(sc: SparkContext, rdd: RDD[Article]) = {
    var iter = 0
    for (
      p <- Seq(30)
    ) {
      val prefs = sc.broadcast(Prefs(iteration = iter,
        termFrequencyThreshold = p

      ))
      //      if (iter > 44)
      ML.multiLabelClassification(prefs, Preprocess.preprocess(prefs, rdd))
      iter += 1
    }
  }
}
