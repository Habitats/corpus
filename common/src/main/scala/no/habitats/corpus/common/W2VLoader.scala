package no.habitats.corpus.common

import no.habitats.corpus.common.CorpusContext.sc
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

object W2VLoader {

  lazy val vectors = loadVectors()

  def fromId(id: String): Option[INDArray] = vectors.get(id)

  def contains(id: String): Boolean = vectors.contains(id)

  def loadVectors(filter: Set[String] = Set.empty): Map[String, INDArray] = {
    Log.v(s"Loading cached W2V vectors (${Config.freebaseToWord2Vec}) ...")
    val start = System.currentTimeMillis
    var vec = sc.textFile(Config.freebaseToWord2Vec)
      .map(_.split(", "))
      .filter(arr => filter.isEmpty || filter.contains(arr(0)))
      .map(arr => (arr(0), arr.toSeq.slice(1, arr.length).map(_.toFloat).toArray))
      .collect() // this takes a long time
      .map(arr => {
        val vector = Nd4j.create(arr._2)
        val id = arr._1
        (id, vector)
      })
      .toMap

    // This is a sanity check, due to a bug with spark and nd4j 3.9 not serializing properly
    // SHOULD ALWAYS BE EQUAL!
    val s = vec.size
    vec = vec.filter(_._2.size(1) == 1000)
    Log.v(s"Filtering from $s to ${vec.size}")

    Log.v(s"Loaded vectors in ${System.currentTimeMillis() - start} ms")
    vec
  }
}
