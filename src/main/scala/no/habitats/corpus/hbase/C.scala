package no.habitats.corpus.hbase

object C {
  val quorum = "corpus"
  val articlesId = "articles_small"
  val annotationId = "annotations_small"
  val nyFamily = "nyt"
  val googleFamily = "google"

  val delim = ";;;"
  val delim2 = ":::"
  val id = "id"

  // article
  val headline = "headline"
  val body = "body"
  val wordCount = "wordCount"
  val date = "date"
  val classifiers = "classifiers"
  val predictions = "predictions"
  val iptc = "iptc"
  val url = "url"
  val descriptors = "descriptors"
  val annotationCount = "annotations"

  // annotations
  val index = "index"
  val phrase = "phrase"
  val salience = "salience"
  val mentionCount ="mentionCount"
  val offset = "offset"
  val freeBaseId = "freeBaseId"
  val wikidataId = "wikidataId"
  val tfIdf = "tfIdf"
  val idTfIdf ="idTfIdf"
}
