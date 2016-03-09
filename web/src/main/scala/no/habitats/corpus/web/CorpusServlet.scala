package no.habitats.corpus.web

import no.habitats.corpus.Log
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{CorsSupport, FutureSupport}

import scala.concurrent.ExecutionContext

class CorpusServlet() extends BackendStack with FutureSupport with JacksonJsonSupport with CorsSupport {
  protected implicit def executor = ExecutionContext.Implicits.global

  protected implicit val jsonFormats: Formats = DefaultFormats

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  before() {
    contentType = formats("json")
  }

  get("/hello/?") {
    contentType = formats("txt")
    Log.v("hello!")
    "HELLO MR. ADMIN"
  }
}
