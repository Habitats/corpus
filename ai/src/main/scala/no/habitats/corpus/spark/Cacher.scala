package no.habitats.corpus.spark

import java.util.Collections
import java.util.Collections

import scala.collection.JavaConverters._
import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.models.Article
import org.apache.spark.rdd.RDD

import scala.util.Random

object Cacher extends RddSerializer {

  /** Fetch json RDD and compute IPTC and annotations */
  def annotateAndCacheArticles() = {
    val annotations = Spotlight.dbpediaAnnotations
    val rdd = Fetcher.rddMini.map(a => Spotlight.toDBPediaAnnotated(a, annotations))
    saveAsText(rdd.map(Article.toStringSerialized), "nyt_corpus_annotated")
  }

  def annotateAndCacheArticlesConfidence() = {
    val db25 = Spotlight.dbpediaAnnotationsMini25
    var rdd = Fetcher.miniCorpus.map(a => Spotlight.toDBPediaAnnotated(a, db25)).map(_.toMinimal).filter(_.ann.nonEmpty)
    saveAsText(rdd.map(Article.toStringSerialized), "nyt_mini_train_annotated_25")
    val db50 = Spotlight.dbpediaAnnotationsMini50
    rdd = Fetcher.miniCorpus.map(a => Spotlight.toDBPediaAnnotated(a, db50)).map(_.toMinimal).filter(_.ann.nonEmpty)
    saveAsText(rdd.map(Article.toStringSerialized), "nyt_mini_train_annotated_50")
    val db75 = Spotlight.dbpediaAnnotationsMini75
    rdd = Fetcher.miniCorpus.map(a => Spotlight.toDBPediaAnnotated(a, db75)).map(_.toMinimal).filter(_.ann.nonEmpty)
    saveAsText(rdd.map(Article.toStringSerialized), "nyt_mini_train_annotated_75")
    val db100 = Spotlight.dbpediaAnnotationsMini100
    rdd = Fetcher.miniCorpus.map(a => Spotlight.toDBPediaAnnotated(a, db100)).map(_.toMinimal).filter(_.ann.nonEmpty)
    saveAsText(rdd.map(Article.toStringSerialized), "nyt_mini_train_annotated_100")
  }

  def scrambler() = {
    for {
      n <- Seq("train", "test", "validation")
      c <- Seq(25, 50, 75, 100, 0)
    } {
      val s = if (c != 0) s"confidence/nyt_mini_${n}_ordered_${c}.txt" else s"nyt_${n}_ordered.txt"
      saveAsText(Fetcher.by(s).map(Article.toStringSerialized).sortBy(a => Math.random), s)
    }
  }

  def computeAndCacheDBPediaAnnotationsToJson(rdd: RDD[Article]) = {
    Spotlight.cacheDbpedia(rdd, 0.25, w2vFilter = true)
//    Spotlight.cacheDbpedia(rdd, 0.5, w2vFilter = true)
//    Spotlight.cacheDbpedia(rdd, 0.75, w2vFilter = true)
//    Spotlight.cacheDbpedia(rdd, 1, w2vFilter = true)
  }

  def annotateAndCacheArticlesWithTypes() = {
    val annotations = Spotlight.dbpediaAnnotationsWithTypes
    val rdd = Fetcher.annotatedTrainOrdered.map(a => Spotlight.toDBPediaAnnotatedWithTypes(a, annotations))
    saveAsText(rdd.map(Article.toStringSerialized), "nyt_train_ordered_types")
    val rdd2 = Fetcher.subTrainOrdered.map(a => Spotlight.toDBPediaAnnotatedWithTypes(a, annotations))
    saveAsText(rdd2.map(Article.toStringSerialized), "subsampled_train_ordered_types")
  }

  def cacheMinimalArticles(rdd: RDD[Article], name: String) = {
    val minimal = rdd
      .filter(_.iptc.nonEmpty)
      .map(_.filterAnnotation(an => an.fb != Config.NONE && W2VLoader.contains(an.fb)))
      .map(_.toMinimal)
      .filter(_.ann.nonEmpty)
      .map(Article.toStringSerialized)
    saveAsText(minimal, name + "_minimal")
  }

  def cacheMiniCorpus() = {
    val rdd = sc.parallelize(Fetcher.rdd.map(Corpus.toIPTC).filter(_.iptc.nonEmpty).takeOrdered(100000)(Ordering.by(_.id.toInt * -1)), Config.partitions)
    saveAsText(rdd.map(Article.toStringSerialized), "nyt_mini_ordered")
  }

  def cacheBalanced() = {
    val train = Fetcher.annotatedTrainOrdered
    val validation = Fetcher.annotatedValidationOrdered
    val test = Fetcher.annotatedTestOrdered
    val splits = Seq("train" -> train, "validation" -> validation, "test" -> test)
    splits.foreach { case (kind, rdd) =>
      rdd.cache()
      IPTC.topCategories
        //      Set("weather")
        .foreach(c => {
        val balanced = createBalanced(c, rdd).filter(_.iptc.nonEmpty)
        saveAsText(balanced.map(Article.toStringSerialized), s"${IPTC.trim(c)}_${kind}_balanced")
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

  def cacheAndSplitTime() = cacheAndSplit(Fetcher.annotatedValidationOrdered, 10, a => a.id.toInt, "nyt_test_time")

  def cacheAndSplitLength() = cacheAndSplit(Fetcher.annotatedTestOrdered, 10, a => a.wc, "nyt_test_length")

  def cacheAndSplit(rdd: RDD[Article], parts: Int, criterion: Article => Double, name: String) = {
    rdd.cache()
    val count = rdd.count.toInt
    val bucketSize = count / parts
    val ordered = rdd.sortBy(criterion).map(_.id).collect()
    val ids: Map[Int, Set[String]] = (0 until parts).map(p => (p, ordered.slice(p * bucketSize - 1, (p + 1) * bucketSize).toSet)).toMap
    ids.map { case (k, v) => (k, rdd.filter(a => v.contains(a.id)).map(Article.toStringSerialized)) }.foreach { case (k, v) => saveAsText(v, s"${name}_$k") }
    rdd.unpersist()
  }

  def cacheSuperSampled(maxLimit: Option[Int] = None) = {
    val rdd = Fetcher.annotatedTrainOrdered
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

    saveAsText(combined.map(Article.toStringSerialized), "supersampled" + maxLimit.map("_" + _).getOrElse(""))
    rdd.unpersist()
  }

  def splitAndCacheBuckets(buckets: Int = 20, name: String, ordering: Article => Int): IndexedSeq[Unit] = {
    val trainFraction: Double = 0.6
    val testValFraction: Double = 1 - trainFraction
    // Assume 60, 20, 20 split, where each model is trained 60 % and every bucket has the equivalent of 20 %
    // Thus; s = train% * s + #buckets * 2 * test% * s
    val all = Fetcher.annotatedRddMini
    all.cache()
    val numAll = all.count
    val ids: Array[Int] = all.map(ordering).collect.sorted
    val s = numAll / (testValFraction * buckets + trainFraction)
    val numTrain = (trainFraction * s).round.toInt
    val numTest, numVal = ((testValFraction * s) /  2).round.toInt
    val trainIds = ids.slice(0, numTrain).toSet
    saveAsText(all.filter(a => trainIds.contains(ordering(a))).map(Article.toStringSerialized), s"$name/nyt_${name}_${buckets}_train")
    for (i <- 0 until buckets) yield {
      val from = numTrain + i * (numTest + numVal)
      val until = numTrain + (i + 1) * (numTest + numVal)
      val bucketIds = Random.shuffle(ids.slice(from.toInt, until.toInt).toSet)
      val testIds = bucketIds.slice(0, bucketIds.size / 2)
      val valIds = bucketIds.slice(bucketIds.size / 2, bucketIds.size)
      saveAsText(all.filter(a => testIds.contains(ordering(a))).map(Article.toStringSerialized), s"$name/nyt_${name}_${buckets}-${i}_test")
      saveAsText(all.filter(a => valIds.contains(ordering(a))).map(Article.toStringSerialized), s"$name/nyt_${name}_${buckets}-${i}_validation")
    }
  }

  def cacheSingleSubSampled(rdd: RDD[Article], name: String) = {
    rdd.cache()
    val pairs = IPTC.topCategories.map(c => (c, rdd.filter(_.iptc.contains(c))))
    rdd.unpersist()
    val counts = pairs.map { case (k, v) => (k, v.count) }.toMap
    val min = counts.values.min.toInt
    val subSampled = pairs.map { case (k, v) => sc.parallelize(v.take(min)) }
    val combined: RDD[Article] = subSampled.reduce(_ ++ _).distinct().sortBy(a => Math.random)
    saveAsText(combined.map(Article.toStringSerialized), s"subsampled_$name")
  }

  def splitAndCache() = {
    val rdd = TC(Fetcher.annotatedRddMini).computed
    rdd.cache()
    splitOrdered(rdd)
    splitShuffled(rdd)
  }

  def cmon() = {
    val s100 = Fetcher.miniMini100.map(_.id).collect().toSet
    val r50 = Fetcher.miniMini50.filter(a => s100.contains(a.id))
    val r25 = Fetcher.miniMini25.filter(a => s100.contains(a.id))
    val r75 = Fetcher.miniMini75.filter(a => s100.contains(a.id))
    saveAsText(r25.map(Article.toStringSerialized), "nyt_mini_train_annotated_25")
    saveAsText(r50.map(Article.toStringSerialized), "nyt_mini_train_annotated_50")
    saveAsText(r75.map(Article.toStringSerialized), "nyt_mini_train_annotated_75")
  }

  def splitOrdered(rdd: RDD[Article], name: String = ""): Unit = {
    // Ordered split
    val numArticles = rdd.count()
    val ids = rdd.map(_.id.toInt).collect.sorted
    val trainIds = ids.slice(0, (numArticles * 0.6).toInt).map(_.toString).toSet
    val testIds = ids.slice((numArticles * 0.6).toInt, (numArticles * 0.8).toInt).map(_.toString).toSet
    val validationIds = ids.slice((numArticles * 0.8).toInt, numArticles.toInt).map(_.toString).toSet

    val train = rdd.filter(a => trainIds.contains(a.id)).map(Article.toStringSerialized).sortBy(a => Math.random)
    saveAsText(train, "nyt_train_ordered" + name)
    val test = rdd.filter(a => testIds.contains(a.id)).map(Article.toStringSerialized).sortBy(a => Math.random)
    saveAsText(test, "nyt_test_ordered" + name)
    val validation = rdd.filter(a => validationIds.contains(a.id)).map(Article.toStringSerialized).sortBy(a => Math.random)
    saveAsText(validation, "nyt_validation_ordered" + name)
  }

  def splitShuffled(rdd: RDD[Article]): Unit = {
    // Shuffled splits
    val splits = rdd.map(Article.toStringSerialized).sortBy(a => Math.random).randomSplit(Array(0.6, 0.2, 0.2), Config.seed)
    rdd.unpersist()
    saveAsText(splits(0), "nyt_train_shuffled")
    saveAsText(splits(1), "nyt_validation_shuffled")
    saveAsText(splits(2), "nyt_test_shuffled")
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
