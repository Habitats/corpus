package no.habitats.corpus.web

import no.habitats.corpus.common.models.{Annotation, Entity}
import no.habitats.corpus.common.{Config, CorpusAPI, Log, W2VLoader}
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{CorsSupport, ScalatraServlet}

import scala.concurrent.ExecutionContext
import scala.util.Try

class CorpusServlet extends ScalatraServlet with JacksonJsonSupport with CorsSupport with CorpusAPI {
  protected implicit def executor = ExecutionContext.Implicits.global

  protected implicit val jsonFormats: Formats = DefaultFormats

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  before() {
    contentType = formats("json")
  }

  def parseParams: (String, Double) = {
    val params: Seq[String] = multiParams("splat").mkString.split("/")
    val text: String = params.head
    val confidence: Double = Try(params(1).toDouble).getOrElse(0.5)
    (text, confidence)
  }

  get("/predict/*") {
    contentType = formats("txt")
    Log.v("hello!")
    val (text, confidence) = parseParams
    predict(text, confidence).mkString("\n")
  }

  get("/extract/*") {
    contentType = formats("txt")
    val (text, confidence) = parseParams
    extract(text, confidence).map(Entity.toSingleJson).mkString("\n")
  }

  get("/annotate/*") {
    contentType = formats("txt")
    val (text, confidence) = parseParams
    annotate(text, confidence).map(_.toJson).mkString("\n")
  }

  get("/annotate_types/*") {
    contentType = formats("txt")
    val (text, confidence) = parseParams
    annotateWithTypes(text, confidence).map(_.toJson).mkString("\n")
  }

  get("/file/*") {
    contentType = formats("html")
    val (file, confidence) = parseParams
    val text = Config.testFile(s"nlp/${file}").getLines().mkString("\n")
    detailedInfo(text, confidence)
  }

  get("/info/*") {
    contentType = formats("html")
    val (text, confidence) = parseParams
    detailedInfo(text, confidence)
  }

  get("/w2v/*") {
    contentType = formats("txt")
    val (text, confidence) = parseParams
    extractFreebaseW2V(text, confidence).map(_.toString).mkString("\n")
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

  def detailedInfo(text: String, confidence: Double) = {
    val entities: Seq[Entity] = extract(text, confidence).sortBy(_.offset)
    val annotations: Seq[Annotation] = annotate(text, confidence).sortBy(_.offset)
    val annotationsWithTypes: Seq[Annotation] = annotateWithTypes(text, confidence).sortBy(_.offset)
    val w2v: Seq[String] = annotationsWithTypes.map(_.fb).map(fb => {
      val vec = freebaseToWord2Vec(fb) match {
        case Some(w2v) => w2v.toString
        case None => "NO_MATCH"
      }
      f"$fb%10s -> $vec"
    })

    val predictions: Set[String] = predict(text, confidence)
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
            <h3>Raw Entities</h3>
            <pre>
              {entities.mkString("\n", "\n", "")}
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
        </ul>
      </body>
    </html>
  }

  get("/") {
    contentType = formats("html")
    val longArticle = Config.testFile("nlp/article.txt").getLines().mkString("\n")
    val shortArticle = Config.testFile("nlp/article_short.txt").getLines().mkString("\n")
    val fb = "/m/02bv9"
    <html>
      <body>
        <h1>Corpus annotation API</h1>
        <ul>
          <h2>API Examples</h2>
          <li>
            <h3>Detailed Info</h3>
            <a href={"/file/example.txt"}>Preprocess example</a> <br/>
            <a href={"/file/sports_article.txt"}>Sport example</a> <br/>
            <a href={"/file/sports_article2.txt"}>Sport example2</a> <br/>
            <a href={"/file/sports_article3.txt"}>Sport example3</a> <br/>
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
