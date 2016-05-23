package no.habitats.corpus.common

import no.habitats.corpus.common.dl4j.NeuralPredictor
import no.habitats.corpus.common.models.{Annotation, Article, DBPediaAnnotation, Entity}
import org.nd4j.linalg.api.ndarray.INDArray

trait CorpusAPI {

  /**
    * Predict an IPTC category for a given text
    *
    * @param text to predict
    * @return IPTC categories
    */
  def predict(text: String, confidence: Double): Set[String] = {
    val annotations: Seq[Annotation] = annotate(text, confidence)
    val article = new Article(id = "NO_ID", body = text, ann = annotations.map(v => (v.id, v)).toMap)

    NeuralPredictor.predict(article, "all-ffn-w2v")
  }

  /**
    * Primitive entity extraction
    *
    * @param text
    * @return Collection of entities
    */
  def extract(text: String, confidence: Double): Seq[Entity] = {
    Spotlight.fetchAnnotations(text, confidence)
  }

  /**
    * Annotate a text with versatile annotations linked to Wikidata, DBpedia and Freebase
    *
    * @param text
    * @return Collection of annotations
    */
  def annotate(text: String, confidence: Double): Seq[Annotation] = {
    val db = for {
      entities <- Spotlight.fetchAnnotations(text, confidence).groupBy(_.id).values
      db = new DBPediaAnnotation("NO_ID", mc = entities.size, entities.minBy(_.offset))
      ann = AnnotationUtils.fromDbpedia(db)
    } yield ann
    db.toSeq
  }

  /**
    * Annotate a text with versatile annotations linked to Wikidata, DBpedia and Freebase, including types
    *
    * @param text
    * @return Collection of annotations
    */
  def annotateWithTypes(text: String, confidence: Double): Seq[Annotation] = {
    val db = for {
      entities <- Spotlight.fetchAnnotations(text, confidence).groupBy(_.id).values
      db = new DBPediaAnnotation("NO_ID", mc = entities.size, entities.minBy(_.offset))
      ann = AnnotationUtils.fromDbpedia(db)
      types <- Seq(ann) ++ AnnotationUtils.fromDBpediaType(db)
    } yield types
    db.toSeq
  }

  /**
    * Extract word2vec vectors based on pre-trained Freebase model from a text
    *
    * @param text
    * @return Collection of INDArray's representing each 1000d vector
    */
  def extractFreebaseW2V(text: String, confidence: Double): Seq[INDArray] = {
    val annotated = annotate(text, confidence)
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
