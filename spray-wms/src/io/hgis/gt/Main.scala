package io.hgis.gt

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import spray.routing.SimpleRoutingApp

object Main extends App with SimpleRoutingApp {

  implicit val system = ActorSystem("my-system")

  // the handler actor replies to incoming HttpRequests
  val handler = system.actorOf(Props[GeoTrellisServiceActor], name = "handler")

  IO(Http) ! Http.Bind(handler, interface = "localhost", port = 8080)

//  startServer(interface = "localhost", port = 8080) {
//    path("hello") {
//      get {
//        complete {
//          <h1>Say hello to spray</h1>
//        }
//      }
//    }
//  }
}