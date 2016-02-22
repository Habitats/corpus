package no.habitats.corpus.dl4j

import java.io.File

import no.habitats.corpus.Config
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.Word2Vec

/**
  * Created by mail on 22.02.2016.
  */
object FreebaseW2V {

  def main(args: Array[String]) = {
val stop =     gVec.getStopWords
    val g = gVec
  }

  // load google
  lazy val gModel = new File(Config.dataPath + "w2v/freebase-vectors-skipgram1000-en.bin")
  lazy val gVec: Word2Vec = WordVectorSerializer.loadGoogleModel(gModel, true).asInstanceOf[Word2Vec]

}
