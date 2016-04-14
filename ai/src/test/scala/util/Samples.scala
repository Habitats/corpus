package util

import java.io.File

import no.habitats.corpus.models.Annotation
import no.habitats.corpus.Corpus
import no.habitats.corpus.common.Config

trait Samples {

  lazy val annotations: Map[String, Seq[Annotation]] = Annotation.fromGoogle(new File(Config.testPath + "/google-annotations/nyt-mini.txt"))
  lazy val articles = Corpus.articlesFromXML(Config.testPath + "/nyt/")
}
