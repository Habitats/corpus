/**
  * Created by Patrick on 13.11.2015.
  */

import no.habitats.corpus.npl.WikiData
import WikiData._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class FreebaseTest extends FunSuite {

  test("freebase --> wikidata") {
    val fb1 = "/m/022b_3" //  José Hernández - https://www.freebase.com/m/022b_3 - https://www.wikidata.org/wiki/Q377623
    val fb2 = "/m/01snm" //   Cincinnati - https://www.freebase.com/m/01snm - https://www.wikidata.org/wiki/Q43196
    val fb3 = "/m/04wsz" //   Middle East - https://www.freebase.com/m/04wsz - https://www.wikidata.org/wiki/Q7204
    val fb4 = "herpaderpa" // None

    val wd1 = fbToWiki(fb1)
    val wd2 = fbToWiki(fb2)
    val wd3 = fbToWiki(fb3)
    val wd4 = fbToWiki(fb4)

    assert(wd1 == Some("Q377623"))
    assert(wd2 == Some("Q43196"))
    assert(wd3 == Some("Q7204"))
    assert(wd4.isEmpty)
  }

  test("instance of") {
    val o = instanceOf.get("377623").get.head
    assert(o == "5")
//    computeInstanceOf
//    computeOccupation
  }

  test("id to label") {
    val id1 = instanceOf.get("377623").get.head
    val label = wdIdToLabel(id1)
    assert(label == "human")

    val ids = Set("1186096", "1184808", "1187303", "1132172", "1184225")
    val labels = Set("Smithfield", "McKean", "Republic", "Fayette City", "Wesleyville")
    assert(ids.map(wdIdToLabel) == labels)
  }
}
