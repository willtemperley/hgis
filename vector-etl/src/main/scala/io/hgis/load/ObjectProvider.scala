package io.hgis.load

import javax.persistence.EntityManager

import com.esri.core.geometry.{OperatorExportToWkb, WkbExportFlags}
import com.vividsolutions.jts.geom.Envelope
import io.hgis.domain.GridCell
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO.SiteGrid
import io.hgis.vector.domain.{GriddedEntity, AnalysisUnit, SiteGridDAO}
import org.apache.hadoop.hbase.client.{Put, HTableInterface}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
 * Only ids are provided as a list because the objects are so big.
 * It's better to get them one at time.
 *
 * Created by willtemperley@gmail.com on 13-Oct-15.
 */
trait ObjectProvider[E <: AnalysisUnit] extends ConvertsGeometry {

  val em: EntityManager
  val clazz: Class[E]

  val CF = "cfv".getBytes
  val GEOM = "geom".getBytes
  val GRID_ID = "grid_id".getBytes
  val ENTITY_ID = "entity_id".getBytes

  val wkbExportOp = OperatorExportToWkb.local()

  def getIds: Iterable[Any]

  def getEntity(id: Any): E = {
    em.find(clazz, id)
  }

  def execute(table: HTableInterface) {

    for (siteId <- getIds) {

      println(siteId)
      val site = getEntity(siteId)
      insertGridCells(table, site)

    }
  }

  def getHGrid(env: Envelope): java.util.ArrayList[GridCell] = {

    val envString = "st_setsrid(st_makebox2d(st_makepoint(%s, %s), st_makepoint(%s, %s)), 4326)".format(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

    val q = em.createNativeQuery("SELECT id, geom " +
      "FROM hgrid.h_grid where geom && " + envString , classOf[GridCell])
    q.getResultList.asInstanceOf[java.util.ArrayList[GridCell]]
  }

  def insertGridCells(hTable: HTableInterface, site: E): Unit = {

    //Still messy!
    site.geom = jtsToEsri(site.jtsGeom)
    val gridCells = getHGrid(site.jtsGeom.getEnvelopeInternal)
    val gridGeoms = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
    val gridIds = gridCells.map(f => f.gridId).toArray

    val griddedEntities: List[GriddedEntity] = IntersectUtil.executeIntersect(site.geom, site.entityId, gridGeoms, gridIds)

    for (sg <- griddedEntities) {

      val put = new Put(getRowKey(site.entityId, sg.gridId))
      val ixPaGrid: Array[Byte] = wkbExportOp.execute(WkbExportFlags.wkbExportDefaults, sg.geom, null).array()
      put.add(CF, GRID_ID, Bytes.toBytes(sg.gridId))
      put.add(CF, ENTITY_ID, Bytes.toBytes(site.entityId))
      put.add(CF, GEOM, ixPaGrid)

      hTable.put(put)
    }

    hTable.flushCommits()

//    println(site.name + " done.")
  }

  def getRowKey(wdpaId: Int, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }

}
