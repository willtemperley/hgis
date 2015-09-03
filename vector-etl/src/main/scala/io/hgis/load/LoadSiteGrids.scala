package io.hgis.load

import javax.persistence.EntityManager

import com.esri.core.geometry.OperatorImportFromWkb
import com.google.inject.Guice
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import io.hgis.ConfigurationFactory
import io.hgis.domain.SiteGrid
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client.HTable

import scala.collection.JavaConversions._

/**
 * Re loads all site grids ...
 *
 * Created by will on 24/10/2014.
 */
object LoadSiteGrids {

  val injector = Guice.createInjector(new JPAModule)
  val wktWriter = new WKTWriter
  val wkbWriter = new WKBWriter
  var wkbReader = OperatorImportFromWkb.local()

  def main(args: Array[String]) {

    val em   = injector.getInstance(classOf[EntityManager])

    val wdpaIdQ = em.createNativeQuery("SELECT id from gridgis.site_grid")

    val ids = wdpaIdQ.getResultList

    if (ids.size != 963986) throw new RuntimeException("Wrong number of grids found.")

    val hTable = new HTable(ConfigurationFactory.get, "pa_grid")

    var i = 0
    for (id <- ids) {
      val sg = em.find(classOf[SiteGrid], id)
      val put = SiteGridDAO.toPutJTS(sg, sg.getRowKey)
      put.add(SiteGridDAO.getCF, SiteGridDAO.GEOM, wkbWriter.write(sg.jtsGeom))
      hTable.put(put)
      i += 1

      if (i % 1000 == 0) {
        hTable.flushCommits()
        println(i)
      }
    }

    hTable.flushCommits()
    println("Loaded " + i + " site grids.")
  }

}
