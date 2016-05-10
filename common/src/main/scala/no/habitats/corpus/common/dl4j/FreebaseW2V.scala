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

object FreebaseW2V extends RddSerializer {

  lazy val gModel            = new File(Config.dataPath + "w2v/freebase-vectors-skipgram1000.bin")
  lazy val gVec: WordVectors = WordVectorSerializer.loadGoogleModel(gModel, true)

  def cacheWordVectors(rdd: RDD[Article], confidence: Double) = {
    val vecs = rdd.flatMap(_.ann.values.filter(_.fb != Config.NONE).map(_.fb)).distinct
      .filter(gVec.hasWord).map(fb => (fb, gVec.getWordVector(fb)))
      .map(a => s"${a._1}, ${a._2.toSeq.mkString(", ")}")

    saveAsText(vecs, s"fb_w2v_${confidence}")
  }

  def cacheWordVectorIds(vectors: Map[String, INDArray]) = {
    sc.parallelize(vectors.keys.toSeq)
      .coalesce(1, shuffle = true).saveAsTextFile(Config.cachePath + "w2v_ids" + DateTime.now.secondOfDay.get)
  }
}
