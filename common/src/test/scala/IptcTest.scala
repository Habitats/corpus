/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus.common.IPTC
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IptcTest extends FunSuite {

  test("iptc") {
    val t1 = "accordions"
    val id1 = "20000019"
    val mt1 = "musical instrument"
    val mtop1 = "arts, culture and entertainment"
    assert(IPTC.nytToIptc(t1) == id1)
    assert(IPTC.toIptc(Set(t1)) == Set(mt1))
    assert(IPTC.toBroad(Set(t1), 0) == Set(mtop1))

    // Descriptors to match
    val desc = Set(
      "Acupuncture", //          holistic medicine / health
      "Adoptions", //            health / health
      "Admissions Standards", // none
      "Terrorism", //            terrorism / crime, law and justice
      "Family" //                family / society
    )
    // Exact matches
    val expectedIptc = Set("holistic medicine", "adoption", "terrorism", "family")
    // Broad matches
    val expectedBroad = Set("health", "crime, law and justice", "society")
    val expectedBroadMinus = Set("health treatment", "family", "crime")
    val expectedBroadMinus2 = Set("medicine", "adoption", "terrorism", "family")

    val iptc = IPTC.toIptc(desc)
    val broad = IPTC.toBroad(desc, 0)
    val broadMinus = IPTC.toBroad(desc, 1)
    val broadMinus2 = IPTC.toBroad(desc, 2)

    assert(expectedIptc == iptc)
    assert(expectedBroad == broad)
    assert(expectedBroadMinus == broadMinus)
    assert(expectedBroadMinus2 == broadMinus2)
  }
}
