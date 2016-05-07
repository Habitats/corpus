package no.habitats.corpus.dl4j

import java.io.File

import no.habitats.corpus.common.CorpusContext._
import no.habitats.corpus.common._
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
import org.joda.time.DateTime

object FreebaseW2V extends RddSerializer {

  lazy val gModel            = new File(Config.dataPath + "w2v/freebase-vectors-skipgram1000.bin")
  lazy val gVec: WordVectors = WordVectorSerializer.loadGoogleModel(gModel, true)

  def cacheWordVectors() = {
    val rdd = sc.textFile(Config.combinedIds)
      .map(_.substring(22, 35).trim)
      .filter(gVec.hasWord).map(fb => (fb, gVec.getWordVector(fb)))
      .map(a => f"${a._1}, ${a._2.toSeq.mkString(", ")}")

    saveAsText(rdd, "fb_to_w2v")
  }

  def cacheWordVectorIds() = {
    sc.parallelize(W2VLoader.vectors.keys.toSeq)
      .coalesce(1, shuffle = true).saveAsTextFile(Config.cachePath + "w2v_ids" + DateTime.now.secondOfDay.get)
  }

}
