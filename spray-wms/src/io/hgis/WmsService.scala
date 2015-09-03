package io.hgis

import akka.actor.ActorRefFactory
import spray.routing.HttpService

/**
 * Created by tempehu on 27-Apr-15.
 */
class WmsService extends HttpService {

  override implicit def actorRefFactory: ActorRefFactory = ???

}
