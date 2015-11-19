package io.hgis.rasterize

import java.awt.image.{BufferedImage, WritableRaster}
import java.awt.{Graphics, Point}
import java.io.{File, FileOutputStream, IOException}
import javax.imageio.ImageIO
import javax.swing.{JFrame, JPanel, WindowConstants}

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.{ParseException, WKTReader}
import io.hgis.hgrid.GlobalGrid

import scala.collection.JavaConversions._

import scala.collection.immutable.Range.Inclusive

/**
  * Created by willtemperley@gmail.com on 30-Jun-15.
  */
object RasterSandboxPoly {
  //  private[io.hgis.rasr] var wkt: String = "LINESTRING(8.2781685 47.0604113,8.2780609 47.0604835,8.2780074 47.0605195,8.2780656 47.060572,8.2780936 47.0606383,8.2781062 47.0610193)"
  var wkt: String = "POLYGON((10.689697265625 -25.0927734375, 34.595947265625 -20.1708984375, 38.814697265625 -35.6396484375, 13.502197265625 -39.1552734375, 10.689697265625 -25.0927734375))"
  var width: Int = 1000
  var height: Int = 500
  val tileSize = 1024

  class WritableRasterPlotter(raster: WritableRaster) extends Plotter {

    val pixVal = Array[Int](1)

    def setValue(v: Int) = pixVal(0) = v

    def plot(x: Int, y: Int) {
      try {
        raster.setPixel(x, height - y, pixVal)
      }
      catch {
        case e: ArrayIndexOutOfBoundsException => {
          System.out.println("x: " + x)
          System.out.println("y: " + y)
        }
      }
    }

  }


  @throws(classOf[IOException])
  @throws(classOf[ParseException])
  def main(args: Array[String]) {

    val wktReader: WKTReader = new WKTReader
    val geom = wktReader.read(wkt).asInstanceOf[Polygon]
    val coords = geom.getCoordinates
    val gr = new GlobalGrid(width, height, tileSize)

    val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val ras: WritableRaster = image.getRaster
    val wrp = new WritableRasterPlotter(ras)
    //    for (y <- gr.step) {
    //
    //    }
    rasterizePoly(geom, gr, wrp)

    //Sliding iterates the gaps in the fence :)
    val slide: Iterator[Array[Coordinate]] = coords.sliding(2)
    wrp.setValue(255)
    for (pair <- slide) {

      val a = pair(0)
      val b = pair(1)

      val a1: (Int, Int) = gr.snap(a.getOrdinate(0), a.getOrdinate(1))
      val b1: (Int, Int) = gr.snap(b.getOrdinate(0), b.getOrdinate(1))

      Rasterizer.rasterize(a1._1, a1._2, b1._1, b1._2, wrp)
    }



    val frame: JFrame = new JFrame
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    frame.setSize(width, height)
    frame.setVisible(true)
    //
    //    val image: BufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_BINARY)
    //    val ras: WritableRaster = image.getRaster
    //    val plotter: Plotter = new WritableRasterPlotter(ras)

    val fos = new FileOutputStream(new File("E:/tmp/rasterized.png"))
    ImageIO.write(image, "png", fos)

    val pane: JPanel = new JPanel() {
      protected override def paintComponent(g: Graphics) {
        super.paintComponent(g)
        g.drawImage(image, 0, 0, null)
      }
    }
    frame.add(pane)
  }

  /**
    * Utility method if awt Points are used
    * @param a from a
    * @param b to b
    * @param plotter
    */
  def rasterize(a: Point, b: Point, plotter: Plotter) {
    Rasterizer.rasterize(a.x, a.y, b.x, b.y, plotter)
  }

  def rasterizePoly(geom: Polygon, gr: GlobalGrid, wrp: WritableRasterPlotter) = {

    val env = geom.getEnvelopeInternal

    //Top left and bottom right
    val tl = gr.snap(env.getMinX, env.getMaxY)
    val br = gr.snap(env.getMaxX, env.getMinY)

    //need to get start and end points
    val tlPt = gr.pixelToPoint(tl._1, tl._2)
    val brPt = gr.pixelToPoint(br._1, br._2)
    val gf = new GeometryFactory()

    val yPix = (0 to (tl._2 - br._2)).map(_ * gr.step)

    for (dY <- yPix) {
      println(dY)
      val x: Array[Coordinate] = new Array(2)
      val scanLineY = tlPt.getY - dY + (gr.step / 2)
      x(0) = new Coordinate(tlPt.getX, scanLineY)
      x(1) = new Coordinate(brPt.getX, scanLineY)
      val line = gf.createLineString(x)


      //intersect each scanline
      val ix = geom.intersection(line)
      println(ix)

      wrp.setValue(120)

      if (!ix.isEmpty) {

        val a = ix.getCoordinates()(0)
        val b = ix.getCoordinates()(1)
        //      val b = ix.getGeometryN(1)

        val a1: (Int, Int) = gr.snap(a.getOrdinate(0), a.getOrdinate(1))
        val b1: (Int, Int) = gr.snap(b.getOrdinate(0), b.getOrdinate(1))

        Rasterizer.rasterize(a1._1, a1._2, b1._1, b1._2, wrp)
      }

    }
  }
}
