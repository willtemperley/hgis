package io.hgis.inject

import javax.persistence.{EntityManager, Persistence}

import com.google.inject.AbstractModule
import net.codingwell.scalaguice.ScalaModule

/**
 * Created by willtemperley@gmail.com on 17-Nov-14.
 */
class JPAModule extends AbstractModule with ScalaModule {

  override def configure() = {

    val emf = Persistence.createEntityManagerFactory("grid-domain")
    val em = emf.createEntityManager()
    bind[EntityManager].toInstance(em)

  }
}
