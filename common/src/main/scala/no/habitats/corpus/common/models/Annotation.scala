package no.habitats.corpus.common.models

import no.habitats.corpus.common.Config
import org.nd4j.linalg.api.ndarray.INDArray

case class Annotation(articleId: String,
                      phrase: String, // phrase
                      mc: Int, // mention count
                      offset: Int = -1,
                      fb: String = Config.NONE, // Freebase ID
                      wd: String = Config.NONE, // WikiData ID
                      db: String = Config.NONE,
                      tfIdf: Double = -1, // term frequency, inverse document frequency
                      w2v: Option[INDArray] = None
                     ) extends JSonable {

  lazy val id: String = {
    if (fb != Config.NONE) fb
    else if (wd != Config.NONE) wd
    else if (db != Config.NONE) db
    else phrase
  }

  // Create annotation from WikiDAta ID
  def fromWd(phrase: String): Annotation = copy(phrase = wd + " - " + phrase)

  override def toString: String = f"id: $id%20s > fb: $fb%10s > wb: $wd%10s > offset: $offset%5d > phrase: $phrase%50s > mc: $mc%3d > TF-IDF: $tfIdf%.10f"
}
object Annotation {
  def serialize(a: Annotation) = Array(a.articleId, a.phrase, a.mc, a.offset, a.fb, a.wd, a.db, a.tfIdf).mkString("\t ") + ""

  def deserialize(string: String): Annotation = {
    val s = string.split("\t ")
    Annotation(s(0), s(1), s(2).toInt, s(3).toInt, s(4), s(5), s(6), s(7).toDouble)
  }
}

