package no.habitats.corpus.spark

import com.nytlabs.corpus.NYTCorpusDocument
import no.habitats.corpus.models.{DBPediaAnnotation, Annotation, Article, Entity}
import no.habitats.corpus.{Config, Log}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object Context {

  lazy val sc = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val conf = new SparkConf()
      .setAll(Config.sparkProps.asScala)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Article], classOf[Entity], classOf[Annotation], classOf[NYTCorpusDocument], classOf[DBPediaAnnotation]))
    val sc = new SparkContext(conf)
    Log.v(sc.getConf.toDebugString)
    sc.setLogLevel("ERROR")
    sc
  }
}
