package io.hgis.dump

import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, File, FileOutputStream, PrintWriter}
import javax.imageio.ImageIO

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKTReader
import io.hgis.ConfigurationFactory
import io.hgis.hgrid.GlobalGrid
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by willtemperley@gmail.com on 05-Jun-15.
 *
 */
object ExtractOneBigRaster extends GeometryScanner {

  val wktReader = new WKTReader()
  val grid = new GlobalGrid(51200, 25600, 1024)

  def main(args: Array[String]): Unit = {

    val htable = new HTable(ConfigurationFactory.get, "osm_tile")

    val scan = new Scan
    scan.addFamily("cfv".getBytes)
    val scanner = htable.getScanner(scan)
    val ways = getIterator(scanner)

    val bigImage: BufferedImage = new BufferedImage(51200, 25600, BufferedImage.TYPE_BYTE_GRAY)
    val bigRas = bigImage.getRaster
    val bigFos = new FileOutputStream(new File("target/bigras.png"))


    var i = 0
    for (img <- ways) {

      i += 1

      val v = img.getValue("cfv".getBytes, "image".getBytes)

      if (v != null) {
        val orig = grid.gridIdToOrigin(img.getRow)

        if (orig._1 < 51200) {
          val bais = new ByteArrayInputStream(v)

          val imgR = ImageIO.read(bais)

          val pix = imgR.getRaster.getPixels(0, 0, grid.tileSize, grid.tileSize, new Array[Int](grid.tileSize * grid.tileSize))

          println(i + "= " + orig._1 + " : " + orig._2)

          bigRas.setPixels(orig._1, 25600 - orig._2, grid.tileSize, grid.tileSize, pix)
        }
      }


    }

    ImageIO.write(bigImage, "TIFF", bigFos)

  }

  def writePNGWfile(name: String, pt: Point): Unit = {
    val pw = new PrintWriter(new File("target/" + name + ".pngw"))


    //    Line 1: A: pixel size in the x-direction in map units/pixel
    //    Line 2: D: rotation about y-axis
    //    Line 3: B: rotation about x-axis
    //    Line 4: E: pixel size in the y-direction in map units, almost always negative[3]
    //    Line 5: C: x-coordinate of the center of the upper left pixel
    //    Line 6: F: y-coordinate of the center of the upper left pixel
    val pixelSize = 360.0 / grid.w
    pw.write("" + pixelSize)
    pw.write("\n")
    pw.write("0")
    pw.write("\n")
    pw.write("0")
    pw.write("\n")
    pw.write("" + pixelSize)
    pw.write("\n")
    pw.write("" + pt.getX)
    pw.write("\n")
    pw.write("" + pt.getY)
    pw.close()
  }

}
