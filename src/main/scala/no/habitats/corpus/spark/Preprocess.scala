package no.habitats.corpus.spark

import no.habitats.corpus._
import no.habitats.corpus.models.{Annotation, Article}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Preprocess {

  def current(rdd: RDD[Article]) = f"A: ${rdd.count}%6s  P: ${rdd.flatMap(_.ann.keys).distinct.count}%6s"

  def wikiDataAnnotations(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    var expanded = rdd
    prefs.value.ontology match {
      case "all" =>
        expanded = expanded.map(a => a.copy(ann = FreeBase.addAnnotations(a.ann, FreeBase.occupations, prefs.value.wikiDataBroadOnly)))
        expanded = expanded.map(a => a.copy(ann = FreeBase.addAnnotations(a.ann, FreeBase.genders, prefs.value.wikiDataBroadOnly)))
        expanded = expanded.map(a => a.copy(ann = FreeBase.addAnnotations(a.ann, FreeBase.instanceOf, prefs.value.wikiDataBroadOnly)))
      case "instanceOf" =>
        expanded = expanded.map(a => a.copy(ann = FreeBase.addAnnotations(a.ann, FreeBase.instanceOf, prefs.value.wikiDataBroadOnly)))
      case "gender" =>
        expanded = expanded.map(a => a.copy(ann = FreeBase.addAnnotations(a.ann, FreeBase.genders, prefs.value.wikiDataBroadOnly)))
      case "occupation" =>
        expanded = expanded.map(a => a.copy(ann = FreeBase.addAnnotations(a.ann, FreeBase.occupations, prefs.value.wikiDataBroadOnly)))
    }
    if (prefs.value.wikiDataBroadOnly) {
      Log.v(s"${current(expanded)} - Added broader WikiData annotations. Using ONLY these.")
    } else {
      Log.v(s"${current(expanded)} - Added broader WikiData annotations. Using ALL.")
    }
    expanded
  }

  def preprocess(sc: SparkContext, prefs: Broadcast[Prefs], raw: RDD[Article]) = {
    var rdd = raw
    Log.v(s"${current(rdd)} - Running preprocessing ...")

    // Filterings ...
    if (prefs.value.wikiDataOnly) {
      rdd = wikiDataFilter(rdd, prefs)
    }
    if (prefs.value.wikiDataIncludeBroad) {
      rdd = wikiDataAnnotations(rdd, prefs)
    }
    rdd = phraseSkipFilter(rdd, prefs)

    // Computations ...
    rdd = computeTfIdf(rdd)
    rdd = addSalience(rdd, prefs)
    rdd = addSalienceWikiData(rdd, prefs)
    if (prefs.value.salientOnly) {
      rdd = salienceFilter(rdd, prefs)
    }

//    IO.cacheAnnotationDistribution(rdd, prefs.value.ontology, prefs.value.iteration)
    Log.v("--- Preprocessing done!")
    rdd
  }

  def computeTfIdf(rdd: RDD[Article]): RDD[Article] = {
    val computed = TC(rdd).computed
    Log.v(s"${current(computed)} - Computed TF-IDF")
    computed
  }

  def salienceFilter(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    val filtered = rdd.map(a => a.copy(ann = a.ann.filter(ann => ann._2.salience > 0))).filter(_.ann.nonEmpty)
    Log.v(s"${current(filtered)} - Removed phrases with salience of 0")
    filtered
  }

  def addSalience(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    val computed = rdd.map(a => a.copy(ann = a.ann.map(annotation => {
      (annotation._1, if (!annotation._2.broad) annotation._2.copy(tfIdf = annotation._2.salientTfIdf(prefs.value.salience)) else annotation._2)
    })))
    Log.v(s"${current(computed)} - Applied salience")
    computed
  }

  def addSalienceWikiData(rdd: RDD[Article], prefs: Broadcast[Prefs]): RDD[Article] = {
    val computed = rdd.map(a => a.copy(ann = a.ann.map(annotation => {
      (annotation._1, if (annotation._2.broad) annotation._2.copy(tfIdf = annotation._2.salientTfIdf(prefs.value.wikiDataBroadSalience)) else annotation._2)
    })))
    Log.v(s"${current(computed)} - Applied salience")
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

  def computeIptc(rdd: RDD[Article], broadMatch: Boolean): RDD[Article] = {
    // compute IPTC, and filter out those with no match
    val filtered = rdd.map(a => a.copy(iptc = broadMatch match {
      case true => IPTC.toBroad(a.desc, 0)
      case false => IPTC.toIptc(a.desc)
    })).filter(_.iptc.nonEmpty)
    Log.v(s"${current(filtered)} - Computed IPTC and filtered articles with no match")
    filtered
  }

}
