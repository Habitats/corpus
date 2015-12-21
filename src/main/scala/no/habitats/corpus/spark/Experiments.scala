package no.habitats.corpus.spark

import no.habitats.corpus.models.Article
import no.habitats.corpus.{Config, Log, Prefs}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Experiments {
  var iter = 0

  def baseline(sc: SparkContext, rdd: RDD[Article]) = {
    var iter = 0
    Config.resultsFileName = "res_baseline"
    Config.resultsCatsFileName = "res_baseline_cats"
    val prefs = sc.broadcast(Prefs())
    ML.multiLabelClassification(prefs, Preprocess.preprocess(sc, prefs, rdd))
    iter += 1
  }

  def salienceExperiment(sc: SparkContext, rdd: RDD[Article]) = {
    var iter = 0
    Config.resultsFileName = "res_salience"
    Config.resultsCatsFileName = "res_salience_cats"
    for (
      so <- Seq(true, false);
      s <- if (!so) -1.0 to 4.5 by 0.5 else Seq(0.0)
    ) {
      val prefs = sc.broadcast(Prefs(iteration = iter,
        salientOnly = so,
        salience = s
      ))
      //      if (iter > 44)
      ML.multiLabelClassification(prefs, Preprocess.preprocess(sc, prefs, rdd))
      iter += 1
    }
  }

  def stats(sc: SparkContext, rdd: RDD[Article]) = {
    Config.resultsFileName = "res_salience"
    Config.resultsCatsFileName = "res_salience_cats"
    val a = Preprocess.preprocess(sc, sc.broadcast(Prefs()), rdd)
    val prefs = sc.broadcast(Prefs(iteration = iter))
    val annCounts = a.flatMap(a => a.iptc.map(c => (c, a.ann.size))).reduceByKey(_ + _).collectAsMap
    Log.r3(annCounts.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "annotation_pr_iptc")
    val artByAnn = a.flatMap(a => a.iptc.map(c => (c, 1))).reduceByKey(_ + _).collectAsMap
    Log.r3(artByAnn.map(c => f"${c._1}%30s ${c._2}%10d").mkString("\n"), "articles_pr_iptc")
    val iptc = a.flatMap(_.iptc).distinct.collect
    val avgAnnIptc = iptc.map(c => (c, annCounts(c).toDouble / artByAnn(c))).toMap
    Log.r3(avgAnnIptc.map(c => f"${c._1}%30s ${c._2}%10.0f").mkString("\n"), "average_ann_pr_iptc")
  }

  def frequencyExperiment(sc: SparkContext, rdd: RDD[Article]) = {
    Config.resultsFileName = "res_salience"
    Config.resultsCatsFileName = "res_salience_cats"
    var iter = 0
    for (
      p <- Seq(30)
    ) {
      val prefs = sc.broadcast(Prefs(iteration = iter,
        termFrequencyThreshold = p

      ))
      //      if (iter > 44)
      ML.multiLabelClassification(prefs, Preprocess.preprocess(sc, prefs, rdd))
      iter += 1
    }
  }

  def ontologyExperiment(sc: SparkContext, rdd: RDD[Article]) = {
    var iter = 0
    Config.resultsFileName = "res_ontology"
    Config.resultsCatsFileName = "res_ontology_cats"
    var x = "asd"
    for (
      o <- Seq("all", "instanceOf", "gender", "occupation");
      //      o <- Seq("all");
      wdb <- Seq(true);
      wdbo <- Seq(true, false);
      wdbs <- if (wdb && !wdbo) (-1.0 until 5d by 0.25) ++ (5d to 50d by 5d) else Seq(0.0)
    ) {
      if (x != o) {
        Log.r("Ontology: " + o)
        Log.r2("Ontology: " + o)
        x = o
      }
      val prefs = sc.broadcast(Prefs(iteration = iter,
        wikiDataIncludeBroad = wdb,
        wikiDataBroadOnly = wdbo,
        wikiDataBroadSalience = wdbs,
        ontology = o
      ))
      ML.multiLabelClassification(prefs, Preprocess.preprocess(sc, prefs, rdd))
      iter += 1
    }
  }

  def everythingExperiment(sc: SparkContext, rdd: RDD[Article]) = {
    var iter = 0
    Config.resultsFileName = "res_everything"
    Config.resultsCatsFileName = "res_everything_cats"
    for (
      wdb <- Seq(true);
      p <- 10 to 60 by 5;
      o <- Seq("instanceOf");
      wdbs <- (-2d until 5d by 0.25)
    ) {
      val prefs = sc.broadcast(Prefs(iteration = iter,
        //        salientOnly = so,
        termFrequencyThreshold = p,
        wikiDataIncludeBroad = wdb,
        wikiDataBroadSalience = wdbs
      ))
      //      if (iter > 44)
      ML.multiLabelClassification(prefs, Preprocess.preprocess(sc, prefs, rdd))
      iter += 1
    }
  }

  def dataQuantityExperiment(sc: SparkContext, rdd: RDD[Article]) = {
    Config.resultsFileName = "res_data_quantity"
    Config.resultsCatsFileName = "res_data_quantity_cats"
    var iter = 0
    for (
      i <- 5000 to 105000 by 5000
    ) {
      val prefs = sc.broadcast(Prefs(iteration = iter))
      //      if (iter > 44)
      val limit = rdd.take(i)
      ML.multiLabelClassification(prefs, Preprocess.preprocess(sc, prefs, sc.parallelize(limit)))
      iter += 1
    }
  }

  def freebaseToWikiDataExperiment(sc: SparkContext, rdd: RDD[Article]) = {
    Config.resultsFileName = "res_wikidata"
    Config.resultsCatsFileName = "res_wikidata_cats"
    var iter = 0
    for (
      wdo <- Seq(true)
    ) {
      val prefs = sc.broadcast(Prefs(iteration = iter,
        wikiDataOnly = wdo
      ))
      //      if (iter > 44)
      ML.multiLabelClassification(prefs, Preprocess.preprocess(sc, prefs, rdd))
      iter += 1
    }
  }
}
