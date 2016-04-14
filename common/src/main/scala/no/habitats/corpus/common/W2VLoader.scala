package no.habitats.corpus.common

import no.habitats.corpus.common.CorpusContext.sc
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

object W2VLoader {

  lazy val vectors = loadVectors()

  def fromId(id: String): Option[INDArray] = vectors.get(id)

  def contains(id: String): Boolean = vectors.contains(id)

  def loadVectors(filter: Set[String] = Set.empty): Map[String, INDArray] = {
    val start = System.currentTimeMillis
    val vec = sc.textFile(Config.dataPath + "nyt/fb_w2v_0.5.txt")
      .map(_.split(", "))
      .filter(arr => filter.isEmpty || filter.contains(arr(0)))
      .map(arr => (arr(0), arr.toSeq.slice(1, arr.length).map(_.toFloat).toArray))
      .map(arr => {
        val vector = Nd4j.create(arr._2)
        val id = arr._1
        (id, vector)
      })
      .collect() // this takes a long time
      .toMap

    Log.v(s"Loaded vectors in ${System.currentTimeMillis() - start} ms")
    vec
  }
}
