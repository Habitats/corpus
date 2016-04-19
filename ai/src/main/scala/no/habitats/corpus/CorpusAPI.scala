package no.habitats.corpus

import no.habitats.corpus.common.{CorpusContext, NeuralModelLoader, W2VLoader}
import no.habitats.corpus.dl4j.NeuralPredictor
import no.habitats.corpus.models.{Annotation, Article, DBPediaAnnotation, Entity}
import no.habitats.corpus.npl.{IPTC, Spotlight}
import org.nd4j.linalg.api.ndarray.INDArray

trait CorpusAPI {

  /**
    * Predict an IPTC category for a given text
    *
    * @param text to predict
    * @return IPTC categories
    */
  def predict(text: String): Set[String] = {
    val annotations: Seq[Annotation] = annotate(text)
    val article = new Article(id = "NO_ID", body = text, ann = annotations.map(v => (v.id, v)).toMap)

    NeuralPredictor.predict(article)
  }

  /**
    * Primitive entity extraction
    *
    * @param text
    * @return Collection of entities
    */
  def extract(text: String): Seq[Entity] = {
    Spotlight.fetchAnnotations(text)
  }

  /**
    * Annotate a text with versatile annotations linked to Wikidata, DBpedia and Freebase
    *
    * @param text
    * @return Collection of annotations
    */
  def annotate(text: String): Seq[Annotation] = {
    val db = for {
      entities <- Spotlight.fetchAnnotations(text).groupBy(_.id).values
      db = new DBPediaAnnotation("NO_ID", mc = entities.size, entities.minBy(_.offset))
      ann = Annotation.fromDbpedia(db)
    } yield ann
    db.toSeq
  }

  /**
    * Extract word2vec vectors based on pre-trained Freebase model from a text
    *
    * @param text
    * @return Collection of INDArray's representing each 1000d vector
    */
  def extractFreebaseW2V(text: String): Seq[INDArray] = {
    val annotated = annotate(text)
    annotated
      .map(_.fb)
      .map(W2VLoader.fromId)
      .filter(_.isDefined)
      .map(_.get)
  }

  /**
    * Extract word2vec vector from Freebase ID
    *
    * @param id - Freebase ID, e.g. "/m/02pqtw"
    * @return 1000d INDArray's representing a single vector
    */
  def freebaseToWord2Vec(id: String): Option[INDArray] = {
    W2VLoader.fromId(id)
  }
}
