package no.habitats.corpus.web

import no.habitats.corpus.CorpusAPI
import no.habitats.corpus.common.{Config, Log, W2VLoader}
import no.habitats.corpus.models.{Annotation, Entity}
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
    predict(text).mkString("\n")
  }

  get("/extract/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    extract(text).map(_.toJson).mkString("\n")
  }

  get("/annotate/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    annotate(text).map(_.toJson).mkString("\n")
  }

  get("/annotate_types/:text/?") {
    contentType = formats("txt")
    val text = params.get("text").get
    annotateWithTypes(text).map(_.toJson).mkString("\n")
  }

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
      case Some(w2v) => W2VLoader.toString(w2v)
      case None => "NO_MATCH"
    }
  }

  get("/vec/size/?") {
    contentType = formats("txt")
    W2VLoader.featureSize.toString
  }

  get("/info/:text/?") {
    contentType = formats("html")
    val text = params.get("text").get
    val entities: Seq[Entity] = extract(text)
    val annotations: Seq[Annotation] = annotate(text)
    val annotationsWithTypes: Seq[Annotation] = annotateWithTypes(text)
    val w2v: Seq[String] = annotationsWithTypes.map(_.fb).map(fb => {
      val vec = freebaseToWord2Vec(fb) match {
        case Some(w2v) => w2v.toString
        case None => "NO_MATCH"
      }
      f"$fb%10s -> $vec"
    })

    val predictions: Set[String] = predict(text)
    <html>
      <body>
        <h1>Detailed info</h1>
        <h2>Article</h2>
        <p>
          {text}
        </p>
        <ul>
          <li>
            <h3>Predicted Category</h3>
            <pre>
              {predictions.mkString("\n", "\n", "")}
            </pre>
          </li>
          <li>
            <h3>Annotations</h3>
            <pre>
              {annotations.map(_.toString).mkString("\n", "\n", "")}
            </pre>
          </li>
          <li>
            <h3>Annotations with Types</h3>
            <pre>
              {annotationsWithTypes.map(_.toString).mkString("\n", "\n", "")}
            </pre>
          </li>
          <li>
            <h3>Freebase W2V Vectors</h3>
            <pre>
              {w2v.map(_.toString).mkString("\n", "\n", "")}
            </pre>
          </li>
          <li>
            <h3>Raw Entities</h3>
            <pre>
              {entities.map(_.toJson).mkString("\n", "\n", "")}
            </pre>
          </li>
        </ul>
      </body>
    </html>
  }

  get("/") {
    contentType = formats("html")
    val longArticle = Config.testFile("npl/article.txt").getLines().mkString("\n")
    val shortArticle = Config.testFile("npl/article_short.txt").getLines().mkString("\n")
    val sport1 = Config.testFile("npl/sports_article.txt").getLines().mkString("\n")
    val sport2 = Config.testFile("npl/sports_article2.txt").getLines().mkString("\n")
    val sport3 = Config.testFile("npl/sports_article3.txt").getLines().mkString("\n")
    val fb = "/m/02bv9"
    <html>
      <body>
        <h1>Corpus annotation API</h1>
        <ul>
          <h2>API Examples</h2>
          <li>
            <h3>Detailed Info</h3>
            <a href={"/info/" + sport1}>Sport example</a> <br/>
            <a href={"/info/" + sport2}>Sport2 example</a> <br/>
            <a href={"/info/" + sport3}>Sport3 example</a> <br/>
            <a href={"/info/" + shortArticle}>Short example</a> <br/>
            <a href={"/info/" + longArticle}>Long example</a>
          </li>
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
            <h3>Annotate with Types</h3>
            <a href={"/annotate_types/" + shortArticle}>Short example</a> <br/>
            <a href={"/annotate_types/" + longArticle}>Long example</a>
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
