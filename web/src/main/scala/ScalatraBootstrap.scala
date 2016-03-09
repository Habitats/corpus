import javax.servlet.ServletContext

import no.habitats.corpus.Log
import no.habitats.corpus.web.CorpusServlet
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {

  override def init(context: ServletContext) {
    context.mount(new CorpusServlet(), "/*")
  }

  override def destroy(context: ServletContext): Unit = {
    super.destroy(context)
    Log.v("Destroying servlet ...")
  }
}
