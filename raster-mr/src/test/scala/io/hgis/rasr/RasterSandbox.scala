package io.hgis.rasr

import java.awt.{Graphics, Point}
import java.awt.image.{BufferedImage, WritableRaster}
import java.io.{File, FileOutputStream, IOException}
import javax.imageio.ImageIO
import javax.swing.{JPanel, JFrame, WindowConstants}

import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import com.vividsolutions.jts.io.{ParseException, WKTReader}
import io.hgis.hgrid.GlobalGrid

/**
 * Created by willtemperley@gmail.com on 30-Jun-15.
 */
object RasterSandbox {
//  private[io.hgis.rasr] var wkt: String = "LINESTRING(8.2781685 47.0604113,8.2780609 47.0604835,8.2780074 47.0605195,8.2780656 47.060572,8.2780936 47.0606383,8.2781062 47.0610193)"
  private[rasr] var wkt: String = "LINESTRING(10.689697265625 -25.0927734375, 34.595947265625 -20.1708984375, 38.814697265625 -35.6396484375, 13.502197265625 -39.1552734375, 10.689697265625 -25.0927734375)"
  private[rasr] var width: Int = 2000
  private[rasr] var height: Int = 1000
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
    val geom: Geometry = wktReader.read(wkt)
    val coords = geom.getCoordinates
    val gr = new GlobalGrid(width, height, tileSize)

    val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val ras: WritableRaster = image.getRaster
    val wrp = new WritableRasterPlotter(ras)
    wrp.setValue(255)

    //Sliding iterates the gaps in the fence :)
    val slide: Iterator[Array[Coordinate]] = coords.sliding(2)
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
//
    val w1: Point = new Point(20, 180)
    val w2: Point = new Point(40, 20)
    val w3: Point = new Point(80, 40)
    val w4: Point = new Point(120, 20)
    val w5: Point = new Point(140, 180)
//
    rasterize(w1, w2, wrp)
    rasterize(w2, w3, wrp)
    rasterize(w3, w4, wrp)
    rasterize(w4, w5, wrp)

    wrp.setValue(100)

    wrp.plot(w1.getX.toInt, w1.getY.toInt)
    wrp.plot(w2.getX.toInt, w2.getY.toInt)
    wrp.plot(w3.getX.toInt, w3.getY.toInt)
    wrp.plot(w4.getX.toInt, w4.getY.toInt)
    wrp.plot(w5.getX.toInt, w5.getY.toInt)

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
}
