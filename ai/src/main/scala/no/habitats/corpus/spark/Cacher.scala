package no.habitats.corpus.spark

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.models.Article
import org.apache.spark.rdd.RDD

object Cacher extends RddSerializer {

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
      .collect.toMap
    val idLabeled: Set[String] = labels(true)
    val idOther: Set[String] = labels(false).take(idLabeled.size)
    // Need to shuffle the examples for training purposes
    all.filter(a => idLabeled.contains(a.id) || idOther.contains(a.id))
  }

  def cacheAndSplitTime() = cacheAndSplit(Fetcher.annotatedTestOrdered, 10, a => a.id.toInt, "nyt_test_time")

  def cacheAndSplitLength() = cacheAndSplit(Fetcher.annotatedTestOrdered, 10, a => a.body.length, "nyt_test_lengths")

  def cacheAndSplit(rdd: RDD[Article], parts: Int, criterion: Article => Double, name: String) = {
    val count = rdd.count.toInt
    val bucketSize = count / parts
    val ordered = rdd.sortBy(criterion).map(_.id).collect()
    val ids: Map[Int, Set[String]] = (0 until parts).map(p => (p, ordered.slice(p * bucketSize - 1, (p + 1) * bucketSize).toSet)).toMap
    ids.map { case (k, v) => (k, rdd.filter(a => v.contains(a.id)).map(JsonSingle.toSingleJson)) }.foreach { case (k, v) => saveAsText(v, s"${name}_$k") }
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
    rdd.unpersist()
  }

  def cacheSingleSubSampled(rdd: RDD[Article], name: String) = {
    rdd.cache()
    val pairs = IPTC.topCategories.map(c => (c, rdd.filter(_.iptc.contains(c))))
    rdd.unpersist()
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

  def cacheSubSampledFiltered() = {
    val rdds = Map(
      "train_w2v" -> Fetcher.annotatedTrainW2V,
      "test_w2v" -> Fetcher.annotatedTestW2V,
      "validation_w2v" -> Fetcher.annotatedValidationW2V
    )
    rdds.foreach { case (k, v) => cacheSingleSubSampled(v, k) }
  }

  def cacheSubSampledShuffled() = {
    val rdds = Map(
      "train_shuffled" -> Fetcher.annotatedTrainShuffled,
      "test_shuffled" -> Fetcher.annotatedTestShuffled,
      "validation_shuffled" -> Fetcher.annotatedValidationShuffled
    )
    rdds.foreach { case (k, v) => cacheSingleSubSampled(v, k) }
  }

  def cacheSubSampledOrdered() = {
    val rdds = Map(
      "train_ordered" -> Fetcher.annotatedTrainOrdered,
      "test_ordered" -> Fetcher.annotatedTestOrdered,
      "validation_ordered" -> Fetcher.annotatedValidationOrdered
    )
    rdds.foreach { case (k, v) => cacheSingleSubSampled(v, k) }
  }

}
