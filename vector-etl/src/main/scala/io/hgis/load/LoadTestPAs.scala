package io.hgis.load

import io.hgis.domain.Site
import io.hgis.vector.domain.SiteDAO
import org.apache.hadoop.hbase.client.{HTableInterface, Put}

import scala.collection.JavaConversions._

/**
 * Loads PAs alongside their grids
 *
 * Created by will on 24/10/2014.
 */
class LoadTestPAs extends GridLoader[Site](classOf[Site]) {

  override def executeLoad(hTable: HTableInterface): Unit = {

    var c = 0
    var i = 0

    for (id <- getIds) {
      i += 1
      val protectedArea = getEntity(id)

      val hGrid = getHGrid(protectedArea.jtsGeom.getEnvelopeInternal)
      c += hGrid.size()

      protectedArea.geom = jtsToEsri(protectedArea.jtsGeom)
      protectedArea.gridCells = hGrid.map(f => jtsWktWriter.write(f.jtsGeom)).toArray
      protectedArea.gridIdList = hGrid.map(f => f.gridId.toString).toArray
      protectedArea.catId = protectedArea.iucnCategory.catId

      val put =  SiteDAO.toPut(protectedArea, protectedArea.getRowKey)

//      val x = put.get(SiteDAO.getCF, "entity_id".getBytes)
//      if (x.length != 8) {
//        throw new RuntimeException("wtf")
//      }

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
    val wdpaIdQ = em.createNativeQuery("SELECT id from protected_sites.wdpa_latest_all where wdpa_id not in (select wdpa_id from hgrid.worst_100) limit 1000")
    val ids = wdpaIdQ.getResultList
    ids.map(_.asInstanceOf[Int].toLong)
  }

  /*
  Subclasses can override this to specialise their hbase rows
   */
  override def addColumns(put: Put, obj: Site): Unit = {

  }
}