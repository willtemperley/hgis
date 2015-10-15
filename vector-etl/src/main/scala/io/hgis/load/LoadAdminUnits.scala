package io.hgis.load

import java.nio.ByteBuffer
import java.util
import javax.persistence.EntityManager

import com.esri.core.geometry.{Geometry, OperatorImportFromWkb}
import com.google.inject.Guice
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import io.hgis.ConfigurationFactory
import io.hgis.domain.{AdminUnit, GridCell}
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.AdminUnitDAO
import org.apache.hadoop.hbase.client.HTable

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Loads PAs alongside their grids
 *
 * hbase org.apache.hadoop.hbase.util.RegionSplitter country UniformSplit -c 14 -f cfv

 *
 * Created by will on 24/10/2014.
 */
object LoadAdminUnits {

  val injector = Guice.createInjector(new JPAModule)

  val wktWriter = new WKTWriter

  val wkbWriter = new WKBWriter

  var wkbReader = OperatorImportFromWkb.local()

  def getHGrid(em: EntityManager, cId: Int) :util.ArrayList[GridCell] = {
    val q = em.createNativeQuery("SELECT id, geom, geohash " +
      "FROM gridgis.h_grid where geom && (select c.geom from administrative_units.country c where c.id  = " + cId + ")", classOf[GridCell])
    q.getResultList.asInstanceOf[util.ArrayList[GridCell]]
  }

  def main(args: Array[String]) {

    val em   = injector.getInstance(classOf[EntityManager])

    val hTable = new HTable(ConfigurationFactory.get, "country")

    var c = 0
    var i = 0

    val adminUnits = em.createQuery("from AdminUnit where geom is not null", classOf[AdminUnit]).getResultList

//    if (adminUnits.size() != 251) throw new RuntimeException

    for (au <- adminUnits) {

      i += 1

      println(au.analysisUnitId)
      val hGrid: util.ArrayList[GridCell] = getHGrid(em, au.analysisUnitId)
      c += hGrid.size()

      au.geom = wkbReader.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(wkbWriter.write(au.jtsGeom)), null)
      au.gridCells = hGrid.map(f => wktWriter.write(f.jtsGeom)).toArray
      au.gridIdList = hGrid.map(f => f.gridId.toString).toArray

      val nextBytes = new Array[Byte](8)
      Random.nextBytes(nextBytes)
      val put =  AdminUnitDAO.toPut(au, nextBytes)
      hTable.put(put)
      if (i % 100 == 0) {
        hTable.flushCommits()
      }
    }

    hTable.flushCommits()
    println("Country grid count: " + c)
  }

}
