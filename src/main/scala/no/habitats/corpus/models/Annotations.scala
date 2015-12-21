package no.habitats.corpus.models

case class Annotations(articleId: String, annotations: Seq[Annotation] = Seq()) {
  override def toString : String = articleId + "\n" + annotations.mkString("\n")
}
