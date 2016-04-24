package no.habitats.corpus.common

import dispatch._
import no.habitats.corpus.common.CorpusContext.sc
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object W2VLoader {

  implicit val formats = Serialization.formats(NoTypeHints)

  lazy val vectors: Map[String, INDArray] = loadVectors()
  lazy val ids    : Set[String]           = Config.dataFile(Config.freebaseToWord2VecIDs).getLines().toSet

  def fromId(id: String): Option[INDArray] = {
    if (Config.useApi) {
      val request = url(Config.corpusApiURL + id).GET
      val res = Await.result(Http(request OK as.String), 15 minutes)
      if (res != "NO_MATCH") {
        val vec = W2VLoader.fromString(res)
        Some(vec)
      } else {
        None
      }
    } else {
      vectors.get(id)
    }
  }

  def contains(id: String): Boolean = {
    ids.contains(id)
  }

  def featureSize(): Int = {
    if (Config.useApi) {
      val request = url(Config.corpusApiURL + "/size").GET
      val res = Await.result(Http(request OK as.String), 15 minutes)
      res.toInt
    } else {
      vectors.values.head.length()
    }
  }

  def toString(w2v: INDArray): String = w2v.data().asFloat().mkString(",")
  def fromString(w2v: String): INDArray = Nd4j.create(w2v.split(",").map(_.toFloat))

  private def loadVectors(filter: Set[String] = Set.empty): Map[String, INDArray] = {
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
    }).toMap

    // This is a sanity check, due to a bug with spark and nd4j 3.9 not serializing properly
    // SHOULD ALWAYS BE EQUAL!
    val s = vec.size
    vec = vec.filter(_._2.size(1) == 1000)
    Log.v(s"Filtering from $s to ${vec.size}")

    Log.v(s"Loaded vectors in ${System.currentTimeMillis() - start} ms")
    vec
  }
}
