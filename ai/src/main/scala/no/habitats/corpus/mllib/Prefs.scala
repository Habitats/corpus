package no.habitats.corpus.mllib

import no.habitats.corpus.common.{Config, IPTC}

case class Prefs(iteration: Int = 0,
                 limit: Int = Int.MaxValue,

                 termFrequencyThreshold: Int = Config.phraseSkipThreshold,
                 wikiDataOnly: Boolean = Config.wikiDataOnly,
                 wikiDataIncludeBroad: Boolean = Config.wikiDataIncludeBroad,
                 ontology: String = "occupation",
                 categories: Seq[String] = IPTC.topCategories
                )

