package no.habitats.corpus.spark

import no.habitats.corpus._
import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.models.{Annotation, Article, DBPediaAnnotation}
import org.apache.spark.rdd.RDD

import scala.util.Random

object Cacher extends RddSerializer {

  def annotateAndCacheArticles(confidence: Double) = {
    val allAnnotations: Map[String, Seq[Annotation]] = DBpediaFetcher.dbpediaAnnotations(confidence)
    val rdd = Fetcher.rddMinimal.map(a => allAnnotations.get(a.id) match {
      case Some(annotations) => a.copy(ann = annotations.map(annotation => (annotation.id, annotation)).toMap)
      case None => a.copy(ann = Map())
    }).filter(_.ann.nonEmpty)
    saveAsText(rdd.map(Article.serialize), s"nyt_corpus_annotated_${confidence}")
  }

  /** Fetch json RDD and compute IPTC and annotations */
  def annotateAndCacheArticles() = {
    Seq(0.25, 0.50, 0.75, 1.0).foreach(confidence => {
      val db = DBpediaFetcher.dbpediaAnnotations(confidence)
      val rdd = Fetcher.miniCorpus.flatMap(a => Spotlight.toDBPediaAnnotated(a, db)).map(_.toMinimal).filter(_.ann.nonEmpty)
      saveAsText(rdd.map(Article.serialize), s"nyt_mini_train_annotated_$confidence")
    })
  }

  def split(rdd: RDD[Article], i: Int) = {
    val clean = rdd.map(_.copy(ann = Map())).map(Article.serialize)
    saveAsText(clean, s"split_$i", partitions = i)
    saveAsText(clean, "nyt_corpus")
  }

  def scrambler() = {
    for {
      n <- Seq("train", "test", "validation")
      c <- Seq(25, 50, 75, 100, 0)
    } {
      val s = if (c != 0) s"confidence/nyt_mini_${n}_ordered_${c}.txt" else s"nyt_${n}_ordered.txt"
      saveAsText(Fetcher.by(s).map(Article.serialize).sortBy(a => Math.random), s)
    }
  }

  def asd = {
    val rdd: Seq[Array[DBPediaAnnotation]] = Seq(0.25, 0.5, 0.75, 1.0).map(confidence => DBpediaFetcher.dbpedia(confidence).take(10))
    rdd.foreach(_.map(DBPediaAnnotation.serialize).foreach(println))
  }

  def computeAndCacheDBPediaAnnotationsToJson(rdd: RDD[Article], partition: Option[Int] = None) = {
    //    Spotlight.cacheDbpedia(rdd, 0.25, w2vFilter = true, partition)
    //    Spotlight.cacheDbpedia(rdd, 0.5, w2vFilter = true)
    Spotlight.cacheDbpedia(rdd, 0.75, w2vFilter = true, partition)
    Spotlight.cacheDbpedia(rdd, 1, w2vFilter = true, partition)
  }

  def annotateAndCacheArticlesWithTypes(confidence: Double = 0.5) = {
    val db = DBpediaFetcher.dbpediaAnnotations(confidence, types = true)
    var rdd = Fetcher.annotatedTestOrdered.collect.flatMap(a => Spotlight.toDBPediaAnnotated(a, db)).filter(_.ann.nonEmpty).map(_.toMinimal)
    saveAsText(sc.parallelize(rdd.map(Article.serialize)), "nyt_test_ordered_types")
    rdd = Fetcher.annotatedTrainOrdered.collect.flatMap(a => Spotlight.toDBPediaAnnotated(a, db)).filter(_.ann.nonEmpty).map(_.toMinimal)
    saveAsText(sc.parallelize(rdd.map(Article.serialize)), "nyt_train_ordered_types")
    rdd = Fetcher.annotatedValidationOrdered.collect.flatMap(a => Spotlight.toDBPediaAnnotated(a, db)).filter(_.ann.nonEmpty).map(_.toMinimal)
    saveAsText(sc.parallelize(rdd.map(Article.serialize)), "nyt_validation_ordered_types")
  }

  def cacheMinimalArticles(rdd: RDD[Article], name: String) = {
    val minimal = rdd
      .filter(_.iptc.nonEmpty)
      .map(_.filterAnnotation(an => an.fb != Config.NONE && W2VLoader.contains(an.fb)))
      .map(_.toMinimal)
      .filter(_.ann.nonEmpty)
      .map(Article.serialize)
    saveAsText(minimal, name + "_minimal")
  }

  def cacheMiniCorpus() = {
    val rdd = sc.parallelize(Fetcher.rdd.map(Corpus.toIPTC).filter(_.iptc.nonEmpty).takeOrdered(100000)(Ordering.by(_.id.toInt * -1)), Config.partitions)
    saveAsText(rdd.map(Article.serialize), "nyt_mini_ordered")
  }

  def cacheBalanced() = {
    val train = Fetcher.annotatedTrainOrdered
    Config.cats.foreach(c => {
      val balanced = subsampledBalanced(c, train).filter(_.iptc.nonEmpty)
      saveAsText(balanced.map(Article.serialize), s"nyt_${IPTC.trim(c)}_balanced")
    })
  }

  /** Create a new dataset with all articles with a given label, and the same amount of randomly sampled articles other labels */
  def subsampledBalanced(label: String, all: RDD[Article]): RDD[Article] = {
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

  def supersampledBalanced(label: String, all: RDD[Article]): RDD[Article] = {
    Log.v(s"Performing supersampling of $label ...")
    val total: Int = all.count.toInt
    val labelIds: Array[Article] = all.filter(_.iptc.contains(label)).collect()
    if (labelIds.isEmpty) throw new IllegalStateException(s"Cannot supersample $label with no candidates!")
    val missing = total - labelIds.size
    val r = new Random(0)
    val fillers = for {
      i <- 0 until missing
      toCopy = labelIds(r.nextInt(labelIds.size))
    } yield toCopy.copy(id = s"${toCopy.id}_$i")
    (sc.parallelize(fillers, Config.partitions) ++ all).repartition(Config.partitions * 2).sortBy(a => Math.random)
  }

  def cacheSupersampledBalanced() = for (c <- Config.cats) {
    val balanced: RDD[Article] = supersampledBalanced(c, Fetcher.annotatedTrainOrdered)
    saveAsText(balanced.map(Article.serialize), s"nyt_${c}_superbalanced")
  }

  def cacheAndSplitTime() = RelativeSplitter.split(Fetcher.annotatedRddMinimal, 10, a => a.id.toInt, "time")

  def cacheAndSplitLength() = StaticSplitter.split(Fetcher.annotatedRddMinimal, 10, a => a.wc, "length")

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

    saveAsText(combined.map(Article.serialize), "supersampled" + maxLimit.map("_" + _).getOrElse(""))
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
    saveAsText(combined.map(Article.serialize), s"subsampled_$name")
  }

  def splitAndCache() = {
    val rdd = Fetcher.annotatedTypes
    rdd.cache()
    splitOrdered(rdd)
    //    splitShuffled(rdd)
    rdd.unpersist()
  }

  def cmon() = {
    val s100 = Fetcher.miniMini100.map(_.id).collect().toSet
    val r50 = Fetcher.miniMini50.filter(a => s100.contains(a.id))
    val r25 = Fetcher.miniMini25.filter(a => s100.contains(a.id))
    val r75 = Fetcher.miniMini75.filter(a => s100.contains(a.id))
    saveAsText(r25.map(Article.serialize), "nyt_mini_train_annotated_25")
    saveAsText(r50.map(Article.serialize), "nyt_mini_train_annotated_50")
    saveAsText(r75.map(Article.serialize), "nyt_mini_train_annotated_75")
  }

  def splitOrdered(rdd: RDD[Article], name: String = ""): Unit = {
    // Ordered split
    val numArticles = rdd.count()
    val ids = rdd.map(_.id.toInt).collect.sorted
    val trainIds = ids.slice(0, (numArticles * 0.6).toInt).map(_.toString).toSet
    val testIds = ids.slice((numArticles * 0.6).toInt, (numArticles * 0.8).toInt).map(_.toString).toSet
    val validationIds = ids.slice((numArticles * 0.8).toInt, numArticles.toInt).map(_.toString).toSet

    val train = rdd.filter(a => trainIds.contains(a.id))
    saveAsText(train.map(Article.serialize).sortBy(a => Math.random), "nyt_train_ordered" + name)
    val test = rdd.filter(a => testIds.contains(a.id))
    saveAsText(test.map(Article.serialize).sortBy(a => Math.random), "nyt_test_ordered" + name)
    val validation = rdd.filter(a => validationIds.contains(a.id))
    saveAsText(validation.map(Article.serialize).sortBy(a => Math.random), "nyt_validation_ordered" + name)

    savePhrases(train, validation, test, name)
  }

  def savePhrases(train: RDD[Article], validation: RDD[Article], test: RDD[Article], name: String): Set[String] = {
    val trainIds = train.flatMap(_.ann.keySet).collect().toSet
    val testIds = train.flatMap(_.ann.keySet).collect().toSet
    val validationIds = train.flatMap(_.ann.keySet).collect().toSet
    val phrasesToKeep: Set[String] = trainIds.union(testIds ++ validationIds)
    Log.saveToFile(phrasesToKeep.mkString("\n"), Config.cachePath + s"excluded_ids_$name.txt")
    phrasesToKeep
  }

  def splitShuffled(rdd: RDD[Article]): Unit = {
    // Shuffled splits
    val splits = rdd.map(Article.serialize).sortBy(a => Math.random).randomSplit(Array(0.6, 0.2, 0.2), Config.seed)
    rdd.unpersist()
    saveAsText(splits(0), "nyt_train_shuffled")
    saveAsText(splits(1), "nyt_validation_shuffled")
    saveAsText(splits(2), "nyt_test_shuffled")

    //TODO: EXCLUSION SAVING MISSING
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

sealed trait Splitter extends RddSerializer {
  def split(rdd: RDD[Article], buckets: Int, ordering: Article => Int, name: String)
}

object RelativeSplitter extends Splitter {
  def split(rdd: RDD[Article], buckets: Int, ordering: Article => Int, name: String) = {
    val trainFraction: Double = 0.6
    val testValFraction: Double = 1 - trainFraction
    // Assume 60, 20, 20 split, where each model is trained 60 % and every bucket has the equivalent of 20 %
    // Thus; s = train% * s + #buckets * 2 * test% * s
    rdd.cache()
    val numAll = rdd.count
    val ids = rdd.map(ordering).collect.sorted
    val s = numAll / (testValFraction * buckets + trainFraction)
    val numTrain = (trainFraction * s).round.toInt
    val numTest, numVal = ((testValFraction * s) / 2).round.toInt
    val trainIds = ids.slice(0, numTrain).toSet
    val train: RDD[Article] = rdd.filter(a => trainIds.contains(a.id.toInt))
    saveAsText(train.map(Article.serialize), s"$name/nyt_${name}_${buckets}_train")
    val exclusionIds = (for (i <- 0 until buckets) yield {
      val from = numTrain + i * (numTest + numVal)
      val until = numTrain + (i + 1) * (numTest + numVal)

      val bucketIds = Random.shuffle(ids.slice(from.toInt, until.toInt).toSet)
      val testIds = bucketIds.slice(0, bucketIds.size / 2)
      val valIds = bucketIds.slice(bucketIds.size / 2, bucketIds.size)

      val test: RDD[Article] = rdd.filter(a => testIds.contains(ordering(a)))
      val validation: RDD[Article] = rdd.filter(a => valIds.contains(ordering(a)))
      saveAsText(test.map(Article.serialize), s"$name/nyt_${name}_${buckets}-${i}_test")
      saveAsText(validation.map(Article.serialize), s"$name/nyt_${name}_${buckets}-${i}_validation")

      (test.flatMap(_.ann.map(_._2.id)) ++ validation.flatMap(_.ann.map(_._2.id))).collect().toSet

    }).reduce(_ ++ _).intersect(train.flatMap(_.ann.map(_._2.id)).collect().toSet)
    Log.saveToFile(exclusionIds.mkString("\n"), Config.cachePath + s"$name/excluded_ids_$name.txt")
    rdd.unpersist()
  }
}

object StaticSplitter extends Splitter {
  def split(rdd: RDD[Article], buckets: Int, ordering: Article => Int, name: String) = {
    rdd.cache()
    val count = rdd.count.toInt
    val bucketSize = count / buckets
    val ordered = rdd.sortBy(ordering).map(_.id).collect()
    val ids: Map[Int, Set[String]] = (0 until buckets).map(p => (p, ordered.slice(p * bucketSize - 1, (p + 1) * bucketSize).toSet)).toMap
    ids.map { case (k, v) => (k, rdd.filter(a => v.contains(a.id)).map(Article.serialize)) }.foreach { case (k, v) => saveAsText(v, s"${name}_$k") }
    rdd.unpersist()
  }
}


