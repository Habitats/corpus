package util

import java.io.File

import no.habitats.corpus.Corpus
import no.habitats.corpus.common.models.Annotation
import no.habitats.corpus.common.{AnnotationUtils, Config}

trait Samples {

  lazy val annotations: Map[String, Seq[Annotation]] = AnnotationUtils.fromGoogle(new File(Config.testPath + "/google-annotations/nyt-mini.txt"))
  lazy val articles = Corpus.articlesFromXML(Config.testPath + "/nyt/")
}
