package no.habitats.corpus.web

import no.habitats.corpus.CorpusAPI
import no.habitats.corpus.common.Log
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{CorsSupport, ScalatraServlet}

import scala.concurrent.ExecutionContext

class CorpusServlet extends ScalatraServlet with JacksonJsonSupport with CorsSupport with CorpusAPI {
  protected implicit def executor = ExecutionContext.Implicits.global

  protected implicit val jsonFormats: Formats = DefaultFormats

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  before() {
    contentType = formats("json")
  }

  get("/predict/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    Log.v("hello!")
    predict(text).mkString(", ")
  }

  get("/extract/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    extract(text).map(_.toString).mkString("\n")
  }

  get("/annotate/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    annotate(text).map(_.toString).mkString("\n")
  }

  /**
    * Extract word2vec vectors based on pre-trained Freebase model from a text
    *
    * @return Collection of INDArray's representing each 1000d vector
    */
  get("/w2v/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    extractFreebaseW2V(text).map(_.toString).mkString("\n")
  }

  get("/vec/m/:id/?") {
    contentType = formats("txt")
    val id = "/m/" + params.get("id").get
  }



  get("/vec/m/:id/?") {
    contentType = formats("txt")
    val id = "/m/" + params.get("id").get
    freebaseToWord2Vec(id) match {
      case Some(w2v) => w2v.toString()
      case None => "NO_MATCH"
    }
  }

  get("/") {
    contentType = formats("html")
    val shortArticle = "Pierre Vinken, 61 years old, will join the board as a nonexecutive director Nov. 29. Mr. Vinken is chairman of Elsevier N.V., the Dutch publishing group."
    val longArticle = "The nation's Roman Catholic priests will not miss the year 2002, their annus horribilis. A year ago, few could have imagined the disrepute into which the priesthood would slip following hundreds of sexual abuse cases involving clergy and a clueless response by bishops who misidentified exactly whom they were supposed to be shepherding.\nThe anger was intense enough to destroy not just a few ecclesiastical careers but also the goodwill of parishioners and the public that priests used to take for granted. Almost forgotten are my former colleagues, the hard-working core of priests who are not malefactors. These men remain trapped in a system where they have next to nothing to say about the shape of Catholic leadership or its response to the crisis.\nLittle wonder that priests' numbers are dwindling. Their experience, their personal holiness and their spiritual insight often don't seem to count. The hierarchy seeks only their silence and deference. As priests see one bishop after another imposed from above to put in place policies without input from the clergy or the laity, they become resigned, disgusted and just plain tired.\nAt the same time, a smaller group of clergy ambitious for higher office have long brought all their skills to the challenging task of pleasing their omnipotent superiors rather than responding to the promptings of their subordinates or of the laity.\nIn the more than two decades I spent as a priest (I left the clergy a decade ago over the issue of celibacy), I had many opportunities to observe the ways priests are required to grovel to their superiors. Once, back in the seminary, as a hundred or so of us stood around waiting for His Eminence the cardinal to appear for an event, a student approached one of the monsignors. ''So, it seems the cardinal is late?'' he asked.\n''Excuse me, young man,'' he was told, ''the cardinal is never late. Everyone else is early.''\nSome years later, when I was head of the seminary's student body, I found myself seated next to the archbishop at a dinner. Our student council had recently completed a study of issues affecting seminary life and our future as priests and human beings. I was eager to share its results with the authorities, and here I was, sitting at dinner next to Himself!\nBut as soon as I broached the topic, the cardinal silenced me. I was not to approach him directly, he said, but only through the appropriate channels so that the chain of authority would be unbroken. He had no desire to know firsthand what his future priests were thinking.\nBishops anointed ''by the favor of the Apostolic See,'' as the Vatican terms it, are deferred to not because of their competence or learning, but because of that favor. This is true today, 40 years after the Second Vatican Council sought to encourage a more collegial style of leadership -- one seeking input from clergy and parishioners and even acknowledging the laity as part of the priesthood.\nAccustomed to this deferential thinking, today's mismanagers of the clerical abuse scandals do not see themselves as ill-intentioned. Ignoring the victims of abuse grows out of an ideology that holds that clergy are different from ordinary people. Accountability is for lesser mortals.\nThe culture of deference to the clerical mystique is deep-rooted. A dozen years ago, I was at a conference for priests on preaching and worship in the context of Vatican II, and the curriculum was suspended one afternoon to make room for an impromptu address by the archbishop. By the end of his hour-long monologue, he had effectively dismissed the newer approaches the faculty had been promoting. Waxing eloquent on the unique power of priests to accomplish things that not even kings and queens could do, he reminded us that even God obeys the words of a priest when he consecrates the bread and wine at mass. There was no rebuttal from the assembled priests.\nThe trouble with deference and silence, of course, is that they encourage ignorance and denial about issues that need to be addressed.\nA few months ago, a group of New York clergy were told by a high-ranking official that he was open to discussing issues directly. However, some of those present told me, it was stressed that this was to be a so-called Roman dialogue, which means: I'll do the talking, you listen.\nThe seeds of the present crisis were really sown in 1968, the year of the papal encyclical known as Humane Vitae, which began the undoing of Vatican II. The encyclical reasserted the church's opposition to artificial contraception and to the principle that church teaching grows and develops. Catholics were not to decide for themselves, as a matter of conscience, whether to use contraception.\nAfter the encyclical, thousands of priests remained silent about this teaching on birth control -- one that was out of sync with the life the faithful lived. Many decided (as the laity had begun to do) that the church's teaching was no real guide for their own sexual lives. Many resigned and sought happiness elsewhere. Others stayed but made their own decisions about licit and illicit sexual relationships -- and were silent about it.\nIs it possible that this silence -- combined with a culture that already valued suppression -- fostered the idea among some bad priests that they could get away with predatory behavior?\nOver the last year, however, the silence has been shattered by public outcry and the flock's rediscovery of its voice. What remains to be seen is whether these voices will be joined by others from within the clergy -- and if they will be allowed to influence the course of Catholic teaching and policy.\nPaul E. Dinter, author of the forthcoming ''The Other Side of the Altar: One Man's Life in the Catholic Priesthood,'' is development director of Care for the Homeless."
    val fb = "/m/02bv9"
    <html>
      <body>
        <h1>Corpus annotation API</h1>
        <ul>
          <h2>API Examples</h2>
          <li>
            <h3>Extract raw entities</h3>
            <a href={"/extract/" + shortArticle}>Short example</a> <br/>
            <a href={"/extract/" + longArticle}>Long example</a>
          </li>
          <li>
            <h3>Annotate</h3>
            <a href={"/annotate/" + shortArticle}>Short example</a> <br/>
            <a href={"/annotate/" + longArticle}>Long example</a>
          </li>
          <li>
            <h3>Word2Vec extraction</h3>
            <a href={"/w2v/" + shortArticle}>Short W2V extraction</a>
          </li>
          <li>
            <h3>Word2Vec</h3>
            <a href={"/vec" + fb}>W2V Vector for
              {fb}
            </a>
          </li>
        </ul>
      </body>
    </html>
  }
}
