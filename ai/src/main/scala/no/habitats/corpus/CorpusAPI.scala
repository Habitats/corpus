package no.habitats.corpus

import no.habitats.corpus.common.{CorpusContext, NeuralModelLoader, W2VLoader}
import no.habitats.corpus.dl4j.NeuralPredictor
import no.habitats.corpus.models.{Annotation, Article, DBPediaAnnotation, Entity}
import no.habitats.corpus.npl.{IPTC, Spotlight}
import org.nd4j.linalg.api.ndarray.INDArray

/**
  * Created by mail on 10.03.2016.
  */
trait CorpusAPI {

  /**
    * Predict an IPTC category for a given text
    *
    * @param text to predict
    * @return IPTC categories
    */
  def predict(text: String): Set[String] = {
    val annotations: Seq[Annotation] = annotate(text)
    val article = new Article(id = "NO_ID", ann = annotations.map(v => (v.id, v)).toMap)
    val iptc = IPTC.topCategories
    val rdd = CorpusContext.sc.parallelize(Seq(article))
    val model = NeuralModelLoader.cachedModel
    val predictors: Map[String, NeuralPredictor] = iptc.map(label => (label, new NeuralPredictor(model, article, label))).toMap
    val results: Set[String] = predictors.map { case (label, predictor) => (label, predictor.correct()) }.filter(_._2).keySet

    results
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
    annotate(text).map(_.fb).map(W2VLoader.fromId).filter(_.isDefined).map(_.get)
  }
}
