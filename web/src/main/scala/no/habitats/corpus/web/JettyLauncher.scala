package no.habitats.corpus.web

import java.util.EventListener

import no.habitats.corpus.common.Config
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

/**
  * Created by mail on 09.03.2016.
  */
object JettyLauncher {
  def main(args: Array[String]) {
    Config.setArgs(args)
    val port = if (System.getenv("PORT") != null) System.getenv("PORT").toInt else 8090
    //    val port = 8090
    val server = new Server(port)
    val context = new WebAppContext()
    context.setContextPath("/")
    context.setResourceBase("src/main/webapp")
    context.setInitParameter(ScalatraListener.LifeCycleKey, "ScalatraBootstrap")
    val listener: EventListener = new ScalatraListener
    context.addEventListener(listener)

    server.setHandler(context)

    server.start()
    server.join()
  }
}
