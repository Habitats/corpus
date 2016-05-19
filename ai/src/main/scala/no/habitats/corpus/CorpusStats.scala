package no.habitats.corpus

import no.habitats.corpus.common.Log
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.mllib.Preprocess
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

case class CorpusStats(rdd: RDD[Article], name: String) {

  val statsFile = s"res/stats_${name}_general.txt"

  def compute() = {
    rdd.cache()
    averagingState(rdd)
    generalStats(rdd)
    articleLabelsStatistics(rdd)
    articleLengthsStatistics(rdd)
    annotationStatistics(rdd)
  }

  def termFrequencyAnalysis(): Unit = {
    var filtered = this.rdd
    for (i <- (0 until 10) ++ (10 until 100 by 10) ++ (100 until 1000 by 100) ++ (1000 until 10000 by 1000)) {
      val phrases = Preprocess.computeTerms(filtered, i)
      if (phrases.nonEmpty) {
        filtered = Preprocess.frequencyFilter(filtered, phrases.toSet).filter(_.ann.nonEmpty)
        val annotationsIptc: RDD[Int] = filtered.map(_.ann.size)
        val stats = f"$i%5d ${filtered.count()}%7d ${phrases.size}%7d ${statsToPretty(annotationsIptc.stats(), "Annotations per article")}"
        Log.toFile(stats, s"res/stats_term_frequency_${name}.txt")
      }
    }
  }

  private def averagingState(rdd: RDD[Article]) = {
    // Annotations per IPTC
    val annCounts = rdd.flatMap(a => a.iptc.map(c => (c, a.ann.size))).reduceByKey(_ + _).collectAsMap
    Log.toFile(annCounts.toSeq.sortBy(_._1).map(c => f"${c._1}%41s ${c._2}%10d").mkString("Annotations per ITPC:\n", "\n", "\n"), statsFile)

    // Articles per IPTC (category distribution)
    val artByAnn = rdd.flatMap(a => a.iptc.map(c => (c, 1))).reduceByKey(_ + _).collectAsMap
    Log.toFile(artByAnn.toSeq.sortBy(_._1).map(c => f"${c._1}%41s ${c._2}%10d").mkString("Articles per IPTC:\n", "\n", "\n"), statsFile)

    // Average ANN per IPTC
    val iptc = rdd.flatMap(_.iptc).distinct.collect.sorted
    val avgAnnIptc = iptc.map(c => (c, annCounts(c).toDouble / artByAnn(c))).toMap
    Log.toFile(avgAnnIptc.toSeq.sortBy(_._1).map(c => f"${c._1}%41s ${c._2}%10.0f").mkString("Average number of annoations per IPTC:\n", "\n", "\n"), statsFile)
  }

  private def generalStats(rdd: RDD[Article]) = {
    // General stats
    val numAnnotations = rdd.flatMap(_.ann.values.toList)
    val numArticles = rdd.count
    Log.toFile(f"Articles:                     ${numArticles}%10d", statsFile)
    Log.toFile(f"Articles without IPTC:        ${rdd.filter(_.iptc.isEmpty).count}%10d", statsFile)
    Log.toFile(f"Articles without annotations: ${rdd.filter(_.ann.isEmpty).count}%10d", statsFile)
    Log.toFile(f"Total annotations:            ${numAnnotations.count}%10d", statsFile)
    Log.toFile(f"Distinct annotations:         ${numAnnotations.map(_.id).distinct.count}%10d", statsFile)
  }

  def lengthCorrelation() = {
    Log.toListFile(rdd.map(a => a.ann.size + " " + a.wc).collect(), "length_correlation")
  }

  private def statsToPretty(stats: StatCounter, name: String): String = f"${name}%30s - Max: ${stats.max.toInt}%10d - Min: ${stats.min.toInt}%3d - Std: ${stats.stdev}%7.2f - Mean: ${stats.mean}%7.2f - Variance: ${stats.variance}%15.2f"

  private def pairs(rdd: RDD[_]): Array[String] = rdd.map(_.toString).map(size => (size, 1)).reduceByKey(_ + _).sortBy(_._2 * -1).map { case (size, count) => f"$size%10s$count%10d" }.collect()

  def annotationStatistics(rdd: RDD[Article]) = {
    val annotationsIptc: RDD[Int] = rdd.map(_.ann.size)
    Log.toFile(statsToPretty(annotationsIptc.stats(), "Annotations per article"), statsFile)
    Log.toListFile(pairs(annotationsIptc), s"res/stats_${name}_annotations_per_article.txt")

    val mentionAnnotation: RDD[Int] = rdd.flatMap(_.ann.values.map(_.id)).map(id => (id, 1)).reduceByKey(_ + _).values
    Log.toFile(statsToPretty(mentionAnnotation.stats(), "Mentions per annotation"), statsFile)
    Log.toListFile(pairs(mentionAnnotation), s"res/stats_${name}_mention_per_annotation.txt")
  }

  def articleLabelsStatistics(rdd: RDD[Article]) = {
    val articlesIptc: RDD[Int] = rdd.map(_.iptc.size)
    Log.toFile(statsToPretty(articlesIptc.stats(), "IPTC"), statsFile)
    Log.toListFile(pairs(articlesIptc), s"res/stats_${name}_iptc_per_article.txt")
  }

  def articleLengthsStatistics(rdd: RDD[Article]) = {
    val articleLength: RDD[Int] = rdd.map(_.wc)
    Log.toFile(statsToPretty(articleLength.stats(), "Article length"), statsFile)
    Log.toListFile(pairs(articleLength), s"res/stats_${name}_article_length.txt")
  }
}
