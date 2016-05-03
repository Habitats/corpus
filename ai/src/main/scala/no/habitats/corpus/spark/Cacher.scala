package no.habitats.corpus.spark

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common.models.Article
import no.habitats.corpus.common._
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD

import scala.collection.Map

object Cacher extends RddSerializer{

  /** Fetch json RDD and compute IPTC and annotations */
  def annotateAndCacheArticles() = {
    val rdd = Fetcher.rdd
      .map(Corpus.toIPTC)
      .map(Corpus.toDBPediaAnnotated)
    saveAsText(rdd.map(JsonSingle.toSingleJson), "nyt_with_all")
  }

  def cacheMinimalArticles() = {
    val minimal = Fetcher.annotatedRdd
      .filter(_.iptc.nonEmpty)
      .map(_.filterAnnotation(an => an.fb != AnnotationUtils.NONE && W2VLoader.contains(an.fb)))
      .filter(_.ann.nonEmpty)
      .map(a => f"${a.id} ${a.iptc.map(IPTC.trim).mkString(",")} ${a.ann.map(_._2.fb).mkString(",")}")
    saveAsText(minimal, "minimal")
  }

  def cacheBalanced() = {
    val train = Fetcher.annotatedTrainW2V
    val validation = Fetcher.annotatedValidationW2V
    val test = Fetcher.annotatedTestW2V
    val splits = Seq("train" -> train, "validation" -> validation, "test" -> test)
    splits.foreach { case (kind, rdd) =>
      rdd.cache()
      IPTC.topCategories
        //      Set("weather")
        .foreach(c => {
        val balanced = createBalanced(c, rdd).filter(_.iptc.nonEmpty)
        saveAsText(balanced.map(JsonSingle.toSingleJson), s"${IPTC.trim(c)}_${kind}_balanced")
      })
    }
  }

  /** Create a new dataset with all articles with a given label, and the same amount of randomly sampled articles other labels */
  def createBalanced(label: String, all: RDD[Article]): RDD[Article] = {
    val labels: Map[Boolean, Set[String]] = all
      .map(a => (a.id, a.iptc))
      .groupBy(_._2.contains(label))
      .map { case (c, ids) => (c, ids.map(_._1).toSet) }
      .collectAsMap()
    val idLabeled: Set[String] = labels(true)
    val idOther: Set[String] = labels(false).take(idLabeled.size)
    // Need to shuffle the examples for training purposes
    all.filter(a => idLabeled.contains(a.id) || idOther.contains(a.id))
  }

  def cacheAndSplitLength() = {
    cacheAndSplitLengths(Fetcher.annotatedTestW2V)
    cacheAndSplitLengths(Fetcher.annotatedValidationW2V)
    cacheAndSplitLengths(Fetcher.annotatedTrainW2V)
  }

  def cacheAndSplitLengths(rdd: RDD[Article]) = {
    val count = rdd.count
    val minIds = rdd.takeOrdered((count * 0.33).toInt)(Ordering.by(_.body.length * -1)).map(_.id).toSet
    val maxIds = rdd.takeOrdered((count * 0.33).toInt)(Ordering.by(_.body.length)).map(_.id).toSet
    val min = rdd.filter(a => minIds.contains(a.id)).map(JsonSingle.toSingleJson)
    val mid = rdd.filter(a => !minIds.contains(a.id) && !maxIds.contains(a.id)).map(JsonSingle.toSingleJson)
    val max = rdd.filter(a => maxIds.contains(a.id)).map(JsonSingle.toSingleJson)
    saveAsText(min, "nyt_length_min.json")
    saveAsText(mid, "nyt_length_mid.json")
    saveAsText(max, "nyt_length_max.json")
  }

  def cacheSuperSampled(maxLimit: Option[Int] = None) = {
    val rdd = Fetcher.annotatedTrainW2V
    rdd.cache()
    var pairs = IPTC.topCategories.map(c => (c, rdd.filter(_.iptc.contains(c))))
    val counts = pairs.map { case (k, v) => (k, v.count) }.toMap
    val max = counts.values.max
    pairs = maxLimit
      .filter(_ > max)
      .map(lim => pairs.map { case (k, v) => if (v.count > lim) (k, sc.parallelize(v.take(lim))) else (k, v) })
      .getOrElse(pairs)

    val superSampled: Seq[RDD[Article]] = pairs.map { case (k, v) => v.union(sc.parallelize(v.takeSample(true, (max - counts(k)).toInt, Config.seed))) }
    val combined: RDD[Article] = superSampled.reduce(_ ++ _).sortBy(a => Math.random)

    saveAsText(combined.map(JsonSingle.toSingleJson), "supersampled" + maxLimit.map("_" + _).getOrElse(""))
  }

  def cacheSingleSubSampled(rdd: RDD[Article], name: String) = {
    rdd.cache()
    val pairs = IPTC.topCategories.map(c => (c, rdd.filter(_.iptc.contains(c))))
    val counts = pairs.map { case (k, v) => (k, v.count) }.toMap
    val min = counts.values.min.toInt
    val subSampled = pairs.map { case (k, v) => sc.parallelize(v.take(min)) }
    val combined: RDD[Article] = subSampled.reduce(_ ++ _).distinct().sortBy(a => Math.random)
    saveAsText(combined.map(JsonSingle.toSingleJson), s"subsampled_$name")
  }

  def splitAndCache() = {
    val rdd = TC(Fetcher.annotatedRdd).computed

    // Ordered split
    val numArticles = rdd.count()
    val ids = rdd.map(_.id.toInt).collect.sorted
    val trainIds = ids.slice(0, (numArticles * 0.6).toInt).map(_.toString).toSet
    val testIds = ids.slice((numArticles * 0.6).toInt, (numArticles * 0.8).toInt).map(_.toString).toSet
    val validationIds = ids.slice((numArticles * 0.8).toInt, numArticles.toInt).map(_.toString).toSet

    val train = rdd.filter(a => trainIds.contains(a.id)).map(JsonSingle.toSingleJson)
    saveAsText(train, "nyt_train_ordered")
    val test = rdd.filter(a => testIds.contains(a.id)).map(JsonSingle.toSingleJson)
    saveAsText(test, "nyt_test_ordered")
    val validation = rdd.filter(a => validationIds.contains(a.id)).map(JsonSingle.toSingleJson)
    saveAsText(validation, "nyt_validation_ordered")

    // Shuffled splits
    val splits = rdd.map(JsonSingle.toSingleJson).sortBy(a => Math.random).randomSplit(Array(0.6, 0.2, 0.2), Config.seed)
    saveAsText(splits(0), "nyt_train_shuffled")
    saveAsText(splits(1), "nyt_validation_shuffled")
    saveAsText(splits(2), "nyt_test_shuffled")
  }

  def cacheSubSampled() = {
    val rdds = Map(
      "train" -> Fetcher.annotatedTrainW2V,
      "test" -> Fetcher.annotatedTestW2V,
      "validation" -> Fetcher.annotatedValidationW2V
    )
    rdds.foreach { case (k, v) => cacheSingleSubSampled(v, k) }
  }



}
