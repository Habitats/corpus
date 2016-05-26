package no.habitats.corpus.common

import java.io.{File, FileNotFoundException}

import no.habitats.corpus.common.CorpusContext.sc
import no.habitats.corpus.common.dl4j.FreebaseW2V
import no.habitats.corpus.common.models.{Annotation, Article}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.ops.transforms.Transforms

import scala.collection.{Map, Set, mutable}

sealed trait VectorLoader {

  def loadVectors(vectorFile: String, filter: Set[String] = Set.empty): Map[String, INDArray] = {
    Log.v(s"Loading cached W2V vectors ($vectorFile) ...")
    if (!new File(vectorFile).exists) throw new FileNotFoundException(s"No cached vectors: $vectorFile")
    val start = System.currentTimeMillis
    var vec = sc.textFile("file:///" + vectorFile)
      .map(_.split(","))
      .filter(arr => filter.isEmpty || filter.contains(arr(0)))
      .map(arr => (arr(0), arr.toSeq.slice(1, arr.length).map(_.toFloat).toArray))
      .map(arr => {
        val vector = Nd4j.create(arr._2)
        val id = arr._1.trim
        (id, vector)
      }).collect.toMap

    // SHOULD ALWAYS BE EQUAL!
    val s = vec.size
    vec = vec.filter(_._2.size(1) == 1000)
    if (s != vec.size) throw new IllegalStateException(s"Vector filtering went wrong. Should be $s, was ${vec.size}!")

    Log.v(s"Loaded vectors in ${System.currentTimeMillis() - start} ms")
    vec
  }

  def fromId(fb: String): Option[INDArray]
  def contains(fb: String): Boolean
  def documentVector(a: Article): INDArray
  def preload(wordVectors: Boolean, documentVectors: Boolean): Unit
}

private class BinaryVectorLoader extends VectorLoader {
  override def fromId(fb: String): Option[INDArray] = if (contains(fb)) Some(Nd4j.create(FreebaseW2V.gVec.getWordVector(fb))) else None
  override def documentVector(a: Article): INDArray = W2VLoader.calculateDocumentVector(a.ann)
  override def contains(fb: String): Boolean = FreebaseW2V.gVec.hasWord(fb)
  override def preload(wordVectors: Boolean, documentVectors: Boolean): Unit = FreebaseW2V.gVec
}

private class TextVectorLoader extends VectorLoader {
  lazy val ids            : Set[String]                                    = Config.dataFile(Config.freebaseToWord2VecIDs).getLines().toSet
  lazy val vectors        : Map[String, INDArray]                          = loadVectors(Config.freebaseToWord2Vec(W2VLoader.confidence))
  lazy val documentVectors: scala.collection.mutable.Map[String, INDArray] = {
    if (!Config.cache) Log.v("Illegal cache access. Cache is disabled!")
    mutable.Map[String, INDArray]() ++ loadVectors(Config.documentVectors(W2VLoader.confidence))
  }

  override def fromId(fb: String): Option[INDArray] = vectors.get(fb)

  override def documentVector(a: Article): INDArray = {
    if (Config.cache) documentVectors.getOrElseUpdate(a.id, W2VLoader.calculateDocumentVector(a.ann))
    else W2VLoader.calculateDocumentVector(a.ann)
  }
  override def contains(fb: String): Boolean = ids.contains(fb)

  override def preload(wordVectors: Boolean, documentVectors: Boolean): Unit = {
    if (Config.cache && documentVectors) this.documentVectors
    if (wordVectors) vectors
  }
}

sealed class SparkVectorLoader extends VectorLoader {

  // Spark broadcasting (it's not pretty but maybe it works?)
  lazy val bVectors        : Broadcast[Map[String, INDArray]] = CorpusContext.sc.broadcast[Map[String, INDArray]](loadVectors(Config.freebaseToWord2Vec(W2VLoader.confidence)))
  lazy val bDocumentVectors: Broadcast[Map[String, INDArray]] = CorpusContext.sc.broadcast[Map[String, INDArray]](loadVectors(Config.documentVectors(W2VLoader.confidence)))

  override def preload(wordVectors: Boolean, documentVectors: Boolean): Unit = {
    bVectors
    bDocumentVectors
  }

  override def contains(fb: String): Boolean = bVectors.value.contains(fb)
  override def fromId(fb: String): Option[INDArray] = bVectors.value.get(fb)
  override def documentVector(a: Article): INDArray = bDocumentVectors.value.getOrElse(a.id, W2VLoader.calculateDocumentVector(a.ann))
}

object W2VLoader extends RddSerializer with VectorLoader {

  implicit val formats              = Serialization.formats(NoTypeHints)
  lazy     val loader: VectorLoader = if (false) new SparkVectorLoader() else new TextVectorLoader()

  // TODO: NOT GOOD
  var confidence  = 0.5
  val featureSize = 1000

  def preload(wordVectors: Boolean, documentVectors: Boolean): Unit = loader.preload(wordVectors, documentVectors)

  def fromId(fb: String): Option[INDArray] = loader.fromId(fb)

  def contains(fb: String): Boolean = loader.contains(fb)

  def documentVector(a: Article): INDArray = loader.documentVector(a)

  def calculateDocumentVector(ann: Map[String, Annotation]): INDArray = {
//    val vectors: Iterable[INDArray] = ann.values.map(an => (an.tfIdf, an.fb)).flatMap { case (tfidf, id) => fromId(id).map(_.mul(tfidf)) }
    val vectors: Iterable[INDArray] = ann.values.map(an => (an.tfIdf, an.fb)).flatMap { case (tfidf, id) => fromId(id).map(_.mul(1)) }
    val combined = vectors.reduce(_.addi(_))
    //    val combined: INDArray = squash(vectors)
    val normalized = Transforms.round(normalize(combined))
    combined
  }

  def normalize(combined: INDArray): INDArray = {
    // Min: -0.15568943321704865
    // Max:  0.15866121649742126
    val max = combined.max(1).getDouble(0)
    val min = combined.min(1).getDouble(0)
    combined.dup().subi(min).divi(max - min)
  }

  def squash(vectors: Iterable[INDArray]): INDArray = vectors.map(_.dup).reduce(_.addi(_)).divi(vectors.size)

  def cacheDocumentVectors(rdd: RDD[Article]) = {
    val docVecs = rdd
      .filter(_.ann.nonEmpty)
      .filter(_.ann.map(_._2.fb).forall(W2VLoader.contains))
      .map(a => s"${a.id},${W2VLoader.toString(calculateDocumentVector(a.ann))}")
    saveAsText(docVecs, s"document_vectors_${confidence}")
  }

  def toString(w2v: INDArray): String = w2v.data().asFloat().mkString(",")
  def fromString(w2v: String): INDArray = Nd4j.create(w2v.split(",").map(_.toFloat))

}