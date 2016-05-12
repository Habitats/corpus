package no.habitats.corpus.mllib

import no.habitats.corpus._
import no.habitats.corpus.common.models.{Annotation, Article}
import no.habitats.corpus.common.{Log, WikiData}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Preprocess {

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

  def preprocess(prefs: Broadcast[Prefs], raw: RDD[Article]) = {
    var rdd = raw
    Log.v(s"${current(rdd)} - Running preprocessing ...")

    rdd = phraseSkipFilter(rdd, prefs)
    rdd = rdd.filter(_.iptc.nonEmpty).filter(_.ann.nonEmpty)

    // Computations ...
    rdd = computeTfIdf(rdd)

    //    IO.cacheAnnotationDistribution(rdd, prefs.value.ontology, prefs.value.iteration)
    Log.v("--- Preprocessing done!")
    rdd
  }

  def computeTfIdf(rdd: RDD[Article]): RDD[Article] = {
    val computed = TC(rdd).computed
    Log.v(s"${current(computed)} - Computed TF-IDF")
    computed
  }

  def phraseSkipFilter(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    val phraseCounts = rdd.flatMap(_.ann.values.map(_.id)).map((_, 1)).reduceByKey(_ + _)
    phraseCounts.cache
    val phrasesToRemove = phraseCounts.filter(_._2 < prefs.value.termFrequencyThreshold).map(_._1).collect.toSet
    def removePhrases(ann: Map[String, Annotation]): Map[String, Annotation] = {
      ann.filter(ann => !phrasesToRemove.contains(ann._1))
    }
    val filtered = rdd.map(a => a.copy(ann = removePhrases(a.ann))).filter(_.ann.nonEmpty)
    Log.v(s"${current(filtered)} - Removed phrases with less than ${prefs.value.termFrequencyThreshold} occurrences")
    filtered
  }

  def wikiDataFilter(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    val filtered = rdd.map(a => a.copy(ann = a.ann.filter(_._2.wd != "NONE"))).filter(_.ann.nonEmpty)
    Log.v(s"${current(filtered)} - Removed phrases with no WikiData match")
    filtered
  }

}