package io.hgis.load

import java.nio.ByteBuffer
import java.util
import javax.persistence.EntityManager

import com.esri.core.geometry.{Geometry, OperatorImportFromWkb}
import com.google.inject.Guice
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import io.hgis.ConfigurationFactory
import io.hgis.domain.GridCell
import io.hgis.domain.Site
import io.hgis.domain.GridCell
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteDAO
import org.apache.hadoop.hbase.client.{HTableInterface, Put, HTable}

import scala.collection.JavaConversions._

/**
 * Loads PAs alongside their grids
 *
 * Created by will on 24/10/2014.
 */
class LoadSites extends GridLoader[Site](classOf[Site]) {


  override def executeLoad(hTable: HTableInterface): Unit = {

    var c = 0
    var i = 0

    for (id <- getIds) {
      i += 1
      val protectedArea = getEntity(id)

      val hGrid: util.ArrayList[GridCell] = getHGrid(protectedArea.jtsGeom.getEnvelopeInternal)
      c += hGrid.size()

      protectedArea.geom = esriWkbReader.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(jtsWkbWriter.write(protectedArea.jtsGeom)), null)
      protectedArea.gridCells = hGrid.map(f => jtsWktWriter.write(f.jtsGeom)).toArray
      protectedArea.gridIdList = hGrid.map(f => f.gridId.toString).toArray

      val put =  SiteDAO.toPut(protectedArea, protectedArea.getRowKey)
      hTable.put(put)
      if (i % 100 == 0) {
        println(i)
        println("n grids = " + c)
        hTable.flushCommits()
      }
    }

    hTable.flushCommits()
  }

  override def getIds: Iterable[Any] = {
    val wdpaIdQ = em.createNativeQuery("SELECT id from protected_sites.wdpa_latest_all where is_designated and wdpa_id not in (select wdpa_id from hgrid.worst_100)")
    val ids = wdpaIdQ.getResultList
    ids.map(_.asInstanceOf[Int].toLong)
  }

  /*
  Subclasses can override this to specialise their hbase rows
   */
  override def addColumns(put: Put, obj: Site): Unit = {

  }
}