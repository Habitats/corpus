package no.habitats.corpus.web

import no.habitats.corpus.CorpusAPI
import no.habitats.corpus.common.{Log, W2VLoader}
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

  get("/w2v/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    extractFreebaseW2V(text).map(_.toString).mkString("\n")
  }

  /**
    *
    */
  get("/vec/:id/?") {
    contentType = formats("txt")
    val id = params.get("id").get
    W2VLoader.fromId(id)
  }

  get("/") {
    contentType = formats("html")
    <html>
      <body>
        <h1>This is some super cool html, and this is a
          <a href="http://reddit.com">link</a>
          !
        </h1>
      </body>
    </html>
  }
}
