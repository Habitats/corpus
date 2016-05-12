package no.habitats.corpus.common

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object CorpusContext {

  lazy val sc = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val conf = new SparkConf()
    if (Config.local) {
       conf.setAll(Config.sparkProps.asScala)
      //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .set("spark.kryoserializer.buffer", "256m")
      //      .set("spark.kryo.registrator", "no.habitats.corpus.spark.CorpusSerializer")
      //      .set("spark.kryo.registrationRequired", "true")
      //      .set("spark.mesos.coarse", "true")
      //      .set("spark.akka.frameSize", "500")
      //      .set("spark.rpc.askTimeout", "30")
    }
    val sc = new SparkContext(conf)
    Log.v(sc.getConf.toDebugString)
    sc.setLogLevel("ERROR")
    sc
  }
}
