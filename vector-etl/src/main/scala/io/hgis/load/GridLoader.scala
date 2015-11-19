package io.hgis.load

import java.util
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
import scala.collection.mutable.ListBuffer

/**
 * Only ids are provided as a list because the objects are so big.
 * It's better to get them one at time.
 *
 * Created by willtemperley@gmail.com on 13-Oct-15.
 */
abstract class GridLoader[E <: AnalysisUnit](val clazz: Class[E], val geomType: Geometry.Type = Geometry.Type.Polygon) extends ConvertsGeometry {

  object GridLoader {
    val CF = "cfv".getBytes
  }

  class ProgressMonitor(initialSize: Int, var pcDone: Int = 0) {

    def updateProgress(idx: Int): Unit = {

      val pc = ((idx * 100) / initialSize.asInstanceOf[Double]).floor.toInt
      if (pc > pcDone) {
        pcDone = pc
        print("\b\b\b\b\b\b\b\b\b\b")
        println(s"$pcDone% complete")
      }

    }

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
    print("\b\b\b\b\b\b\b\b\b\b\b")
    println()
    print(f"${id.toString.toInt}%10d")
    em.find(clazz, id)
  }

  def executeLoad(table: HTableInterface) {

    val ids = getIds.toList
    val progressMonitor = new ProgressMonitor(ids.size)

    table.setAutoFlushTo(false)

    for (analysisUnitId <- ids.zipWithIndex) {

      val analysisUnit = getEntity(analysisUnitId._1)
      if(analysisUnit == null) {
        throw new RuntimeException(clazz.getSimpleName + " was not found with id: " + analysisUnitId)
      }

      val gridCells = getHGrid(analysisUnit.jtsGeom.getEnvelopeInternal)

      val gridCellPutList: Iterable[Put] =  executeGridding(analysisUnit, gridCells)
      gridCellPutList.foreach(table.put)
      table.flushCommits()
      notifyComplete(analysisUnit)

      progressMonitor.updateProgress(analysisUnitId._2)


    }
    table.flushCommits()
  }


  def getHGrid(env: Envelope): java.util.ArrayList[GridCell] = {

    val envString = s"st_setsrid(st_makebox2d(st_makepoint(${env.getMinX}, ${env.getMinY}), st_makepoint(${env.getMaxX}, ${env.getMaxY})), 4326)"
    val q = em.createNativeQuery("SELECT id, geom, is_leaf, parent_id " +
      "FROM hgrid.h_grid_node where is_leaf and geom && " + envString , classOf[GridCell])
    q.getResultList.asInstanceOf[java.util.ArrayList[GridCell]]

  }

  /*
  Called on completion of flush to hbase table
   */
  def notifyComplete(analysisUnit: E): Unit = {}

  def executeGridding(analysisUnit: E, gridCells: util.List[GridCell], buffer: ListBuffer[Put] = new ListBuffer[Put]): ListBuffer[Put] = {

    //Still messy!
    if (analysisUnit.geom == null) {
      analysisUnit.geom = jtsToEsri(analysisUnit.jtsGeom, geomType)
    }
    if (analysisUnit.jtsGeom == null) {
      analysisUnit.jtsGeom = esriToJTS(analysisUnit.geom)
    }

    val gridGeoms = gridCells.map(f => jtsToEsri(f.jtsGeom)).toArray
    val gridIds = gridCells.map(f => f.gridId).toArray

    val griddedEntities: List[GriddedEntity] = IntersectUtil.executeIntersect(analysisUnit, gridGeoms, gridIds, dimensionMask)

    for (sg <- griddedEntities) {
      val put = GriddedObjectDAO.toPut(sg, getRowKey(analysisUnit.entityId, sg.gridId))
      //More specialised classes can add extra columns
      addColumns(put, analysisUnit)
      buffer += put
    }
    buffer
  }

  def getRowKey(wdpaId: Long, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }

}
