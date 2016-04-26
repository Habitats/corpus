package no.habitats.corpus

import no.habitats.corpus.common.Config
import no.habitats.corpus.npl.IPTC

case class Prefs(iteration: Int = 0,
                 limit: Int = Int.MaxValue,

                 termFrequencyThreshold: Int = Config.phraseSkipThreshold,
                 wikiDataOnly: Boolean = Config.wikiDataOnly,
                 wikiDataIncludeBroad: Boolean = Config.wikiDataIncludeBroad,
                 ontology: String = "occupation",
                 categories: Seq[String] = IPTC.topCategories
                )

