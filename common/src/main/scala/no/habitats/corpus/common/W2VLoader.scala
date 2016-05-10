package no.habitats.corpus.common

import java.io.{File, FileNotFoundException}

import no.habitats.corpus.common.CorpusContext.sc
import no.habitats.corpus.common.W2VLoader._
import no.habitats.corpus.common.models.{Annotation, Article}
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.{Map, Set}
import scala.util.Try

case class W2VLoader(confidence: Double) extends RddSerializer {
  lazy val vectors        : Map[String, INDArray] = loadVectors(Config.freebaseToWord2Vec(confidence))
  lazy val documentVectors: Map[String, INDArray] = loadVectors(Config.documentVectors(confidence))
  lazy val ids            : Set[String]           = Try(Config.dataFile(Config.freebaseToWord2VecIDs(confidence)).getLines().toSet).getOrElse(vectors.keySet)
}

object W2VLoader extends RddSerializer {
  def preload() = loader.vectors

  implicit val formats = Serialization.formats(NoTypeHints)

  private var loader: W2VLoader = null

  def setLoader(confidence: Double, iKnowWhatImDoing: Boolean = false) = if (iKnowWhatImDoing || confidence > 1) this.loader = W2VLoader(confidence) else throw new IllegalArgumentException("Do you know what you're doing?")

  def fromId(id: String): Option[INDArray] = loader.vectors.get(id).map(_.dup())

  def featureSize(): Int = loader.vectors.values.head.length()

  def contains(id: String): Boolean = loader.ids.contains(id)

  def calculateDocumentVector(ann: Map[String, Annotation]): INDArray = {
    val vectors = ann.values.map(_.fb).flatMap(fromId).toSeq
    val combined = vectors.reduce(_.addi(_)).divi(vectors.size)
    //    val combined = Nd4j.vstack(vectors: _*).mean(0) // supposed to be fast, but it's 3 times slower
    combined
  }

  def fetchCachedDocumentVector(articleId: String): Option[INDArray] = loader.documentVectors.get(articleId) // If this crashes, you fucked up
  def fetchCachedDocumentVector(articleId: String): Option[INDArray] = loader.documentVectors.get(articleId).map(_.dup()) // If this crashes, you fucked up

  def cacheDocumentVectors(rdd: RDD[Article]) = {
    loader.vectors
    val docVecs = rdd.map(a => s"${a.id},${W2VLoader.toString(calculateDocumentVector(a.ann))}")
    saveAsText(docVecs, s"document_vectors_${loader.confidence}")
  }

  def toString(w2v: INDArray): String = w2v.data().asFloat().mkString(",")
  def fromString(w2v: String): INDArray = Nd4j.create(w2v.split(",").map(_.toFloat))

  private def loadVectors(vectorFile: String, filter: Set[String] = Set.empty): Map[String, INDArray] = {
    Log.v(s"Loading cached W2V vectors ($vectorFile) ...")
    if (!new File(vectorFile).exists) throw new FileNotFoundException(s"No cached vectors: $vectorFile")
    val start = System.currentTimeMillis
    var vec = sc.textFile(vectorFile)
      .map(_.split(","))
      .filter(arr => filter.isEmpty || filter.contains(arr(0)))
      .map(arr => (arr(0), arr.toSeq.slice(1, arr.length).map(_.toFloat).toArray))
      .map(arr => {
        val vector = Nd4j.create(arr._2)
        val id = arr._1
        (id, vector)
      }).collect.toMap

    // SHOULD ALWAYS BE EQUAL!
    val s = vec.size
    vec = vec.filter(_._2.size(1) == 1000)
    if (s != vec.size) throw new IllegalStateException(s"Vector filtering went wrong. Should be $s, was ${vec.size}!")

    Log.v(s"Loaded vectors in ${System.currentTimeMillis() - start} ms")
    vec
  }
}