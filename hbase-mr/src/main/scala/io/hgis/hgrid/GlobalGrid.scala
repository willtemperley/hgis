package io.hgis.hgrid

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by willtemperley@gmail.com on 28-May-15.
 *
 */

class GlobalGrid(val w: Int, val h: Int, val tileSize: Int) {

  val geomFact = new GeometryFactory(new PrecisionModel(), 4326)

  /**
   * Snaps a geographical coordinate to a grid coordinate
   *
   * @param x longitude
   * @param y latitude
   * @return
   */
  def snap(x: Double, y: Double): (Int, Int) = {

    val x1 = (((x + 180) * w) / 360).toInt
    val y1 = (((y + 90) * h)  / 180).toInt

    (x1, y1)
  }


  /**
   * Generates a grid id for a pixel
   *
   * @param x pixel X, geographic origin
   * @param y pixel Y, geographic origin
   * @return
   */
  def gridId(x: Int, y: Int): Array[Byte] = {

    val a = Math.floor(x / tileSize).toInt
    val b = Math.floor(y / tileSize).toInt

    toBytes(a, b)

  }

  //Get the bottom left offset of a grid
  def gridIdToOrigin(id: Array[Byte]): (Int, Int) = {

    val x = keyToPixel(id)
    (x._1 * tileSize, x._2 * tileSize)
  }

  /**
   * Just strings two integers into a byte array
   *
   * @param x x coord
   * @param y y coord
   * @return
   */
  def toBytes(x: Int, y: Int) = Bytes.toBytes(x) ++ Bytes.toBytes(y)

  /**
   * The inverse of pixelToKey
   *
   * @param bytes the strung-together data
   * @return a point as a tuple
   */
  def keyToPixel(bytes: Array[Byte]) = (Bytes.toInt(bytes.slice(0,4)), Bytes.toInt(bytes.slice(4,8)))

  def pixelToPoint(x: Int, y: Int): Point = {

    val x2 = ((x.toDouble * 360) / w) - 180
    val y2 = ((y.toDouble * 180) / h) - 90

    geomFact.createPoint(new Coordinate(x2, y2))
  }

  def toRasterIdx(x: Int, y: Int) = (w * (h - y)) + x

}
