package io.hgis.load

import java.nio.ByteBuffer
import java.util
import javax.persistence.EntityManager

import com.esri.core.geometry.{Geometry, OperatorImportFromWkb}
import com.google.inject.Guice
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import io.hgis.ConfigurationFactory
import io.hgis.domain.{GridCell, Site}
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteDAO
import org.apache.hadoop.hbase.client.HTable

import scala.collection.JavaConversions._

/**
 * Loads PAs alongside their grids
 *
 * Created by will on 24/10/2014.
 */
object LoadSites {

  val injector = Guice.createInjector(new JPAModule)

  val wktWriter = new WKTWriter

  val wkbWriter = new WKBWriter

  var wkbReader = OperatorImportFromWkb.local()

  def getHGrid(em: EntityManager, pa: Site): util.ArrayList[GridCell] = {
    val q = em.createNativeQuery("SELECT id, geom " +
      "FROM hgrid.h_grid where geom && (select w.geom from protected_sites.wdpa_latest_all w where w.id  = " + pa.siteId + ")", classOf[GridCell])
    q.getResultList.asInstanceOf[util.ArrayList[GridCell]]
  }


  def main(args: Array[String]) {

    val em   = injector.getInstance(classOf[EntityManager])

    val wdpaIdQ = em.createNativeQuery("SELECT id from protected_sites.wdpa_latest_all where is_designated")

    val ids = wdpaIdQ.getResultList

    if (ids.size != 206560) throw new RuntimeException("Wrong number of PAs found.")

    val hTable = new HTable(ConfigurationFactory.get, "pa")

    var c = 0
    var i = 0

    for (id <- ids) {
      i += 1
      val protectedArea = DataAccess.em.find(classOf[Site], id)

      println(protectedArea.name)
      val hGrid: util.ArrayList[GridCell] = getHGrid(em, protectedArea)
      c += hGrid.size()

      protectedArea.geom = wkbReader.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(wkbWriter.write(protectedArea.jtsGeom)), null)
      protectedArea.gridCells = hGrid.map(f => wktWriter.write(f.jtsGeom)).toArray
      protectedArea.gridIdList = hGrid.map(f => f.gridId.toString).toArray

      val put =  SiteDAO.toPut(protectedArea, protectedArea.getRowKey)
      hTable.put(put)
      if (i % 100 == 0) {
        hTable.flushCommits()
      }
    }

    hTable.flushCommits()
    println("PA grid count: " + c)
  }

}
