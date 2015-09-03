package io.hgis.op

import com.esri.core.geometry._

import scala.collection.mutable.ListBuffer

/**
 * Attempting clipping instead of intersection
 *
 * Create a factory instead?
 *
 * Created by tempehu on 07-Nov-14.
 */
class ClipUtil {

  val sr = SpatialReference.create(4326)


  def doClip(poly: Geometry, geomList: List[Geometry]): List[Geometry] = {


    val localOp = OperatorClip.local()
    println("accelerating")
    localOp.accelerateGeometry(poly, sr, Geometry.GeometryAccelerationDegree.enumMedium)


    val outList = ListBuffer[Geometry]()
    for (gridCell <- geomList ) {

      val bigPoly = new SimpleGeometryCursor(poly)
      val env = new Envelope2D
      gridCell.queryEnvelope2D(env)

      val out: GeometryCursor = localOp.execute(bigPoly, env, sr, null)
      val iterator = Iterator.continually(out.next).takeWhile(_ != null)
      for (x <- iterator) {
        outList += x
        println(x.calculateArea2D())
      }
    }

    outList.toList
  }
}
