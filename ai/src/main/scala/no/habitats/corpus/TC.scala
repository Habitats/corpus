package no.habitats.corpus

import no.habitats.corpus.common.models.{Annotation, Article}
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * Static methods for misc formulas and measures
  */
case class TC(rdd: RDD[Article]) {
  rdd.cache()
  val documentsWithTerm      = rdd.flatMap(_.ann.values).map(a => (a.id, 1)).reduceByKey(_ + _).collect.toMap
  val maxFrequencyInDocument = rdd.map(a => (a.id, Try(a.ann.map(_._2.mc).max).getOrElse(0))).collect.toMap
  val frequencySumInDocument = rdd.map(a => (a.id, Try(a.ann.map(_._2.mc).sum).getOrElse(0))).collect.toMap
  val documentCount          = rdd.count

  lazy val computed: RDD[Article] = {
    rdd.map(a => {
      val newAnnotations = a.ann.map(annotation => {
        val res = tfidf(annotation._2)
        (annotation._1, annotation._2.copy(tfIdf = res))
      })
      a.copy(ann = newAnnotations)
    })
    rdd.unpersist()
  }

  def tfidf(annotation: Annotation): Double = simple(annotation) * inverseFrequency(annotation)

  def simple(annotation: Annotation): Double = annotation.mc.toDouble / frequencySumInDocument(annotation.articleId)

  def log(annotation: Annotation): Double = 1 + Math.log(annotation.mc)

  def inverseFrequency(annotation: Annotation): Double = Math.log(documentCount.toDouble / documentsWithTerm(annotation.id))

  def inverseFrequencySmooth(annotation: Annotation): Double = Math.log(1 + (documentCount.toDouble / documentsWithTerm(annotation.id)))
}

