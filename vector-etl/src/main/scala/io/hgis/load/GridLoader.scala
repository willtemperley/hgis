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
abstract class GridLoader[E <: AnalysisUnit](clazz: Class[E]) extends ConvertsGeometry {

  object GridLoader {
    val CF = "cfv".getBytes
  }

  val em: EntityManager = DataAccess.em

  val GEOM = "geom".getBytes
  val GRID_ID = "grid_id".getBytes
  val ENTITY_ID = "entity_id".getBytes

  val wkbExportOp = OperatorExportToWkb.local()

  def getIds: Iterable[Any]

  /*
  Subclasses can override this to specialise their hbase rows
   */
  def addColumns(put: Put, obj: E): Unit

  def getEntity(id: Any): E = {
    em.find(clazz, id)
  }

  def executeLoad(table: HTableInterface) {

    val ids = getIds.toList
    val sz = ids.size
    var pcDone = 0

    for (analysisUnitId <- ids.zipWithIndex) {

      val analysisUnit = getEntity(analysisUnitId._1)
      if(analysisUnit == null) {
        throw new RuntimeException(clazz.getSimpleName + " was not found with id: " + analysisUnitId)
      }
      insertGridCells(table, analysisUnit)

      val pc = ((analysisUnitId._2 * 100) / sz.asInstanceOf[Double]).floor.toInt

      if (pc > pcDone) {
        pcDone = pc
        println("%s%% complete".format(pcDone))
      }

    }
  }

  def getHGrid(env: Envelope): java.util.ArrayList[GridCell] = {

    val envString = "st_setsrid(st_makebox2d(st_makepoint(%s, %s), st_makepoint(%s, %s)), 4326)".format(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

    val q = em.createNativeQuery("SELECT id, geom " +
      "FROM hgrid.h_grid where geom && " + envString , classOf[GridCell])
    q.getResultList.asInstanceOf[java.util.ArrayList[GridCell]]
  }

  def insertGridCells(hTable: HTableInterface, analysisUnit: E): Unit = {

    //Still messy!
    analysisUnit.geom = jtsToEsri(analysisUnit.jtsGeom)
    val gridCells = getHGrid(analysisUnit.jtsGeom.getEnvelopeInternal)
    val gridGeoms = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
    val gridIds = gridCells.map(f => f.gridId).toArray

    val griddedEntities: List[GriddedEntity] = IntersectUtil.executeIntersect(analysisUnit, gridGeoms, gridIds)

    for (sg <- griddedEntities) {

      val put = new Put(getRowKey(analysisUnit.entityId, sg.gridId))
      val ixPaGrid: Array[Byte] = wkbExportOp.execute(WkbExportFlags.wkbExportDefaults, sg.geom, null).array()
      put.add(GridLoader.CF, GRID_ID, Bytes.toBytes(sg.gridId))
      put.add(GridLoader.CF, ENTITY_ID, Bytes.toBytes(analysisUnit.entityId))
      put.add(GridLoader.CF, GEOM, ixPaGrid)
      //Flag completely covered

      //More specialised classes can add extra columns
      addColumns(put, analysisUnit)

      hTable.put(put)
    }

    hTable.flushCommits()

//    println(analysisUnit.name + " done.")
  }

  def getRowKey(wdpaId: Int, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }

}
