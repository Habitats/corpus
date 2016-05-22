package no.habitats.corpus.mllib

import no.habitats.corpus._
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common.{Log, WikiData}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Preprocess {
  def frequencyFilter(rdd: RDD[Article], phrases: Set[String]): RDD[Article] = rdd.map(_.filterAnnotation(ann => phrases.contains(ann.id))).filter(_.ann.nonEmpty)

  def computeTerms(rdd: RDD[Article], termFrequencyThreshold: Int): Array[String] = {
    val counted = rdd
      .flatMap(_.ann.keySet.toList)
      .map(ann => (ann, 1))
      .reduceByKey(_ + _)
    val oldSize = counted.distinct.count
    val filtered = counted
      // skip phrases mentioned less than n times
      .filter(_._2 > termFrequencyThreshold)
      .map(_._1).distinct.collect.sorted
    Log.v(s"Phrase frequency filtering reduced unique phrases from ${oldSize} to ${filtered.size}.")
    filtered
  }

  def current(rdd: RDD[Article]) = f"A: ${rdd.count}%6s  P: ${rdd.flatMap(_.ann.keys).distinct.count}%6s"

  def wikiDataAnnotations(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    var expanded = rdd
    prefs.value.ontology match {
      case "all" =>
        expanded = expanded.map(a => a.copy(ann = WikiData.addAnnotations(a.ann, WikiData.occupations)))
        expanded = expanded.map(a => a.copy(ann = WikiData.addAnnotations(a.ann, WikiData.genders)))
        expanded = expanded.map(a => a.copy(ann = WikiData.addAnnotations(a.ann, WikiData.instanceOf)))
      case "instanceOf" =>
        expanded = expanded.map(a => a.copy(ann = WikiData.addAnnotations(a.ann, WikiData.instanceOf)))
      case "gender" =>
        expanded = expanded.map(a => a.copy(ann = WikiData.addAnnotations(a.ann, WikiData.genders)))
      case "occupation" =>
        expanded = expanded.map(a => a.copy(ann = WikiData.addAnnotations(a.ann, WikiData.occupations)))
    }
    Log.v(s"${current(expanded)} - Added broader WikiData annotations. Using ALL.")
    expanded
  }

  def computeTfIdf(rdd: RDD[Article]): RDD[Article] = {
    val computed = TC(rdd).computed
    Log.v(s"${current(computed)} - Computed TF-IDF")
    computed
  }

  def wikiDataFilter(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    val filtered = rdd.map(a => a.copy(ann = a.ann.filter(_._2.wd != "NONE"))).filter(_.ann.nonEmpty)
    Log.v(s"${current(filtered)} - Removed phrases with no WikiData match")
    filtered
  }

}
