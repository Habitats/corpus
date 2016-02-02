package no.habitats.corpus

case class Prefs(iteration: Int = 0,
                 limit: Int = Int.MaxValue,

                 termFrequencyThreshold: Int = Config.phraseSkipThreshold,
                 wikiDataOnly: Boolean = Config.wikiDataOnly,
                 wikiDataIncludeBroad: Boolean = Config.wikiDataIncludeBroad,
                 wikiDataBroadOnly: Boolean = Config.wikiDataBroadOnly,
                 ontology: String = "occupation"
                  )


