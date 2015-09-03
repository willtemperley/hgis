package io.hgis.load

import javax.persistence.EntityManager

import com.google.inject.Guice
import io.hgis.domain.Site
import io.hgis.inject.JPAModule

/**
 * Just holds test data access machinery for use across multiple tests
 *
 * Created by willtemperley@gmail.com on 21-Nov-14.
 */
object DataAccess {

  val injector = Guice.createInjector(new JPAModule)
  val em = injector.getInstance(classOf[EntityManager])


  @deprecated
  def getSite(id: Int): Site = {
    em.createQuery("from Site where wdpaId = " + id, classOf[Site]).getSingleResult
  }

  def getAdminUnit(id: Int): Unit = {
    em.createQuery("from Site where wdpaId = " + id, classOf[Site]).getSingleResult
  }
}
