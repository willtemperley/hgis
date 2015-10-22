package io.hgis.load

import javax.persistence.EntityManager

import com.esri.core.geometry.{Geometry, OperatorExportToWkb, WkbExportFlags}
import com.vividsolutions.jts.geom.Envelope
import io.hgis.domain.GridCell
import io.hgis.hdomain.{GriddedObjectDAO, ConvertsGeometry, GriddedEntity, AnalysisUnit}
import io.hgis.op.IntersectUtil
import io.hgis.vector.domain.SiteGridDAO.SiteGrid
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client.{Put, HTableInterface}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
 * Only ids are provided as a list because the objects are so big.
 * It's better to get them one at time.
 *
 * Created by willtemperley@gmail.com on 13-Oct-15.
 */
abstract class GridLoader[E <: AnalysisUnit](clazz: Class[E], val geomType: Geometry.Type = Geometry.Type.Polygon) extends ConvertsGeometry {

  object GridLoader {
    val CF = "cfv".getBytes
  }

  /*
  Defines what intersection will be retrained, default being only polygon features (4)
   */
  var dimensionMask = geomType match {
    case Geometry.Type.Polyline => 3
    case _ => 4
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

  def insertGridCells(hTable: HTableInterface, analysisUnit: E): Int = {

    //Still messy!
    if (analysisUnit.geom == null) {
      analysisUnit.geom = jtsToEsri(analysisUnit.jtsGeom, geomType)
    }
    if (analysisUnit.jtsGeom == null) {
      analysisUnit.jtsGeom = esriToJTS(analysisUnit.geom)
    }
    val gridCells = getHGrid(analysisUnit.jtsGeom.getEnvelopeInternal)
    val gridGeoms = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
    val gridIds = gridCells.map(f => f.gridId).toArray

//    if(gridIds.length > 0) println("grids: " + gridIds.length)


    val griddedEntities: List[GriddedEntity] = IntersectUtil.executeIntersect(analysisUnit, gridGeoms, gridIds, dimensionMask)

    for (sg <- griddedEntities) {


      val put = GriddedObjectDAO.toPut(sg, getRowKey(analysisUnit.entityId, sg.gridId))
      //new Put(getRowKey(analysisUnit.entityId, sg.gridId))
//      val ixPaGrid: Array[Byte] = wkbExportOp.execute(WkbExportFlags.wkbExportDefaults, sg.geom, null).array()
//      put.add(GridLoader.CF, GRID_ID, Bytes.toBytes(sg.gridId))
//      put.add(GridLoader.CF, ENTITY_ID, Bytes.toBytes(analysisUnit.entityId))
//      put.add(GridLoader.CF, GEOM, ixPaGrid)
      //Flag completely covered

      //More specialised classes can add extra columns
      addColumns(put, analysisUnit)

      hTable.put(put)
    }

    hTable.flushCommits()
    griddedEntities.size

//    println(analysisUnit.name + " done.")
  }

  def getRowKey(wdpaId: Long, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }

}
