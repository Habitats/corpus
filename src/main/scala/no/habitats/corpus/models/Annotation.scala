package no.habitats.corpus.models

import no.habitats.corpus.sources.FreeBase

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
    if (fb == "NONE" && wd == "NONE") throw new Exception("ANNOTATION WITHOUT ID")
    else if (fb != "NONE") fb
    else if (FreeBase.wikiToFbMapping.contains(wd)) FreeBase.wikiToFbMapping(wd)
    else wd
  }

  override def toString: String = f"id: $id%10s > fb: $fb%10s > wb: $wd%10s > offset: $offset%5d > phrase: $phrase%50s > mc: $mc%3d > TF-IDF: $tfIdf%.10f"
}

