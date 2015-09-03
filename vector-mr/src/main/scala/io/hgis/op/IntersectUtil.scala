package io.hgis.op

import com.esri.core.geometry._
import io.hgis.vector.domain.SiteGridDAO
import SiteGridDAO.SiteGrid

import scala.collection.mutable.ListBuffer

/**
 * Polygon intersection utility
 *
 * Created by tempehu on 07-Nov-14.
 */
object IntersectUtil {

  val sr = SpatialReference.create(4326)

  def executeIntersect(poly: Geometry, intersectorGeom: Geometry): Iterator[Geometry] = {

    val inGeoms = new SimpleGeometryCursor(intersectorGeom)
    val intersector = new SimpleGeometryCursor(poly)

    val ix: GeometryCursor = OperatorIntersection.local().execute(inGeoms, intersector, sr, null, 4)

    Iterator.continually(ix.next).takeWhile(_ != null)
  }

  def executeIntersect(poly: Geometry, geomList: Array[Geometry]): Iterator[Geometry] = {

    val bigPoly = new SimpleGeometryCursor(poly)
    val inGeoms = new SimpleGeometryCursor(geomList)

    val localOp = OperatorIntersection.local()
    localOp.accelerateGeometry(poly, sr, Geometry.GeometryAccelerationDegree.enumMedium)
    val outGeoms = localOp.execute(inGeoms, bigPoly, sr, null, 4)

    Iterator.continually(outGeoms.next).takeWhile(_ != null)
  }

  /**
   * Populates a list of SiteGrids with their geom and grid id, ignoring all zero area outputs
   *
   * @param poly the big polygon to intersect
   * @param geomList the intersectors (e.g. a grid)
   * @param gridIds the ids of the intersectors
   * @return
   */
  def executeIntersect(poly: Geometry, geomList: Array[Geometry], gridIds: Array[Int]): List[SiteGrid] = {

    val bigPoly = new SimpleGeometryCursor(poly)
    val inGeoms = new SimpleGeometryCursor(geomList)

    val localOp = OperatorIntersection.local()
    localOp.accelerateGeometry(poly, sr, Geometry.GeometryAccelerationDegree.enumMedium)
    val outGeoms = localOp.execute(inGeoms, bigPoly, sr, null, 4)

    Iterator.continually(outGeoms.next).takeWhile(_ != null).map((f: Geometry) => f)

    val sgs = new ListBuffer[SiteGrid]
    var result = outGeoms.next
    var geomId = outGeoms.getGeometryID
    while (result != null) {
      if (result.calculateArea2D() > 0) {

        val sg = new SiteGrid
        sg.geom = result
        sg.gridId = gridIds(geomId)
        sgs.append(sg)
      }
      result = outGeoms.next
      geomId = outGeoms.getGeometryID
    }
    sgs.toList
  }
}
