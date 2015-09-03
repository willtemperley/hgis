package io.hgis.gt

import javax.persistence.EntityManager

import akka.actor.Actor
import com.google.inject.Guice
import io.hgis.inject.JPAModule

class GeoTrellisServiceActor extends GeoTrellisService with Actor {
  // the HttpService trait (which GeoTrellisService will extend) defines
  // only one abstract member, which connects the services environment
  // to the enclosing actor or test.
  def actorRefFactory = context

  val injector = Guice.createInjector(new JPAModule)
  val em = injector.getInstance(classOf[EntityManager])

  def receive = runRoute(rootRoute)
}