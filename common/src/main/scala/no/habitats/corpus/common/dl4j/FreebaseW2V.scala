package no.habitats.corpus.common.dl4j

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import no.habitats.corpus.common.models.Article
import org.apache.spark.rdd.RDD
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
import org.joda.time.DateTime
import org.nd4j.linalg.api.ndarray.INDArray

import scala.collection.JavaConverters._

object FreebaseW2V extends RddSerializer {

  lazy val gModel            = new File(Config.dataPath + "w2v/freebase-vectors-skipgram1000.bin")
  //  lazy val gModel            = new File("e:Archive2/w2v/freebase-vectors-skipgram1000.bin")
  lazy val gVec: WordVectors = WordVectorSerializer.loadGoogleModel(gModel, true)

  def cacheFbIds() = Log.toListFile(gVec.vocab().words().asScala.toSet, "fb_ids_with_w2v.txt")

  def cacheWordVectors(rdd: RDD[Article], confidence: Double, types: Boolean) = {
    val vecs = rdd
      .flatMap(_.ann.values.filter(_.fb != Config.NONE).map(_.fb)).distinct
      .filter(gVec.hasWord)
      .map(fb => (fb, gVec.getWordVector(fb)))

    saveAsText(vecs.map(a => s"${a._1}, ${a._2.toSeq.mkString(", ")}"), s"fb_w2v_${confidence}_${if (types) "_types" else ""}")
    saveAsText(vecs.keys, s"fb_w2v_${confidence}_${if (types) "_types" else ""}")
    vecs.coalesce(1, shuffle = true).saveAsTextFile(Config.cachePath + s"fb_w2v_ids_${confidence}_${if (types) "_types" else ""}")
  }

  def cacheAll() = {
    val vecs = sc.textFile("file:///" + Config.dataPath + "fb_ids_with_w2v.txt")
      .filter(gVec.hasWord)
      .map(fb => (fb, gVec.getWordVector(fb)))
      .map(a => s"${a._1}, ${a._2.toSeq.mkString(", ")}")

    saveAsText(vecs, "fb_w2v_all")
  }
}
