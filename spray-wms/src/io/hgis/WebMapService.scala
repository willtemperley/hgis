package io.hgis

import akka.actor._
import akka.util.Timeout
import spray.can.Http
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.http._

import scala.concurrent.duration._

class WebMapService extends Actor with ActorLogging {
  implicit val timeout: Timeout = 1.second // for the actor 'asks' // ExecutionContext for the futures and scheduler

  def receive = {
    // when a new connection comes in we register ourselves as the connection handler
    case _: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index

//    case HttpRequest(GET, Uri.Path("/stream"), _, _, _) =>
//      val peer = sender // since the Props creator is executed asyncly we need to save the sender ref
//      context actorOf Props(new Streamer(peer, 25))


    case Timedout(HttpRequest(method, uri, _, _, _)) =>
      sender ! HttpResponse(
        status = 500,
        entity = "The " + method + " request to '" + uri + "' has timed out..."
      )
  }

  ////////////// helpers //////////////

  lazy val index = HttpResponse(
    entity = HttpEntity(`text/html`,
      <html>
        <body>
          <h1>Need to do getcapabilities</h1>
        </body>
      </html>.toString()
    )
  )


}