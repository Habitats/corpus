package no.habitats.corpus.spark

import no.habitats.corpus.models.{Annotation, Article, Entity}
import no.habitats.corpus.{Config, Log}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object Context {

  lazy val sc = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val conf = new SparkConf()
      .setAll(Config.sparkProps.asScala)
      .registerKryoClasses(Array(classOf[Article], classOf[Entity], classOf[Annotation]))
    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
    sc
  }
}
