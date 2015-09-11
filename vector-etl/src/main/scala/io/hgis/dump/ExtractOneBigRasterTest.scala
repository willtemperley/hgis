package io.hgis.dump

import java.awt.image.BufferedImage
import java.io._
import javax.imageio.ImageIO

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKTReader
import io.hgis.ConfigurationFactory
import io.hgis.hgrid.GlobalGrid
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.{GridCoverageFactory, GridCoverage2D}
import org.geotools.gce.geotiff.GeoTiffWriter
import org.geotools.geometry.Envelope2D
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.parameter.{ParameterValueGroup, GeneralParameterValue}

/**
 * Created by willtemperley@gmail.com on 05-Jun-15.
 *
 */
object ExtractOneBigRasterTest extends GeometryScanner {

  val wktReader = new WKTReader()
  val grid = new GlobalGrid(51200, 25600, 1024)

  def main(args: Array[String]): Unit = {


    val bigImage: BufferedImage = new BufferedImage(51200, 25600, BufferedImage.TYPE_BYTE_GRAY)
    val bigRas = bigImage.getRaster

    val files = new File("E:/tmp/ras").listFiles().filter(f => ! f.getName.endsWith("pngw"))

    for (f <- files) {

      val inputStream = new FileInputStream(f)
      val imgR = ImageIO.read(inputStream)


      val fname = f.getName.replace(".png", "")

      val orig = fname.split("_").map(_.toInt)
//
      if (orig(0) < 51200) {

        val bais = new FileInputStream(f)
        val imgR = ImageIO.read(bais)
//
        val orig_i = orig(0)
        val orig_j = 25600 - orig(1) - 1024
//        val orig_j = orig(1)
//        println(orig(1) + " => " + orig_j)

        val pix = imgR.getRaster.getPixels(0, 0, grid.tileSize, grid.tileSize, new Array[Int](grid.tileSize * grid.tileSize))
        val p = pix.grouped(1024).toList.reverse.flatMap(f => f).toArray

        bigRas.setPixels(orig_i, orig_j, grid.tileSize, grid.tileSize, p)
      }


    }

//    ImageIO.write(bigImage, "tiff", bigFos)
//    writePNGWfile("density", 0, 0)

    val bbox = new Envelope2D(DefaultGeographicCRS.WGS84, -180, -90, 360, 180)
    val factory = new GridCoverageFactory()
    // "img" is a java.awt.image.BufferedImage with RGB and Alpha channel
    val coverage = factory.create("tiff", bigImage, bbox)

    val gtw = new GeoTiffWriter(new File("E:/tmp/out.tif"))

    gtw.write(coverage, null)

  }


  def writePNGWfile(name: String, X: Double, Y: Double): Unit = {
    val pw = new PrintWriter(new File("E:/tmp/" + name + ".tifw"))


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
    pw.write("" + X)
    pw.write("\n")
    pw.write("" + Y)
    pw.close()
  }

}
