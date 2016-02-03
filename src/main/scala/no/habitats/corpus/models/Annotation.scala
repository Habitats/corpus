package no.habitats.corpus.models

import no.habitats.corpus.features.WikiData

case class Annotation(articleId: String,
                      index: Int, // index
                      phrase: String, // phrase
                      mc: Int, // mention count
                      offset: Int = -1,
                      fb: String = "NONE", // Freebase ID
                      wd: String = "NONE", // WikiData ID
                      broad: Boolean = false,
                      tfIdf: Double = -1 // term frequency, inverse document frequency
                       ) {

  lazy val id: String = {
    if (fb == "NONE" && wd == "NONE") phrase
    else if (fb != "NONE") fb
    else if (WikiData.wikiToFbMapping.contains(wd)) WikiData.wikiToFbMapping(wd)
    else wd
  }

  override def toString: String = f"id: $id%10s > fb: $fb%10s > wb: $wd%10s > offset: $offset%5d > phrase: $phrase%50s > mc: $mc%3d > TF-IDF: $tfIdf%.10f"
}

