package io.hgis.dump

import java.awt.image.BufferedImage
import java.io._
import javax.imageio.{ImageWriteParam, ImageIO}

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKTReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.hgrid.GlobalGrid
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.io.imageio.GeoToolsWriteParams
import org.geotools.gce.geotiff.{GeoTiffFormat, GeoTiffWriteParams, GeoTiffWriter}
import org.geotools.geometry.Envelope2D
import org.geotools.referencing.CRS
import org.opengis.parameter.{ParameterValueGroup, GeneralParameterValue}

/**
 * Created by willtemperley@gmail.com on 05-Jun-15.
 *
 */
object ExtractDensityRaster extends TableIterator {

  val wktReader = new WKTReader()
  val grid = new GlobalGrid(43200, 21600, 1080)

  def main(args: Array[String]): Unit = {

    val htable = new HTable(ConfigurationFactory.get, "osm_tile")

    val scan = new Scan
    scan.addFamily("cfv".getBytes)
    val scanner = htable.getScanner(scan)
    val ways = getIterator(scanner)

    val bigImage: BufferedImage = new BufferedImage(grid.w, grid.h, BufferedImage.TYPE_BYTE_GRAY)
    val bigRas = bigImage.getRaster


    var i = 0
    for (img <- ways) {

      i += 1

      val v = img.getValue("cfv".getBytes, "image".getBytes)

      if (v != null) {
        val orig = grid.gridIdToOrigin(img.getRow)

        if (orig._1 < grid.w) {

          val bais = new ByteArrayInputStream(v)

          println(i + "= " + orig._1 + " : " + orig._2)

          val imgR = ImageIO.read(bais)
          val pix = imgR.getRaster.getPixels(0, 0, grid.tileSize, grid.tileSize, new Array[Int](grid.tileSize * grid.tileSize))
          //
          val orig_i = orig._1
          val orig_j = grid.h - orig._2 - grid.tileSize

          val p = pix.grouped(grid.tileSize).toList.reverse.flatMap(f => f).toArray

          bigRas.setPixels(orig_i, orig_j, grid.tileSize, grid.tileSize, p)
        }
      }

    }


    //getting the write parameters
    val wp = new GeoTiffWriteParams
    val format = new GeoTiffFormat

      //setting compression to LZW
    wp.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    wp.setCompressionType("LZW")
    wp.setCompressionQuality(1.0F)

    val params = format.getWriteParameters
      params.parameter(
        AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName.toString)
        .setValue(wp)

    val sourceCRS = CRS.decode("EPSG:4326")
    val bbox = new Envelope2D(sourceCRS, -90, -180, 180, 360)
    val coverage = new GridCoverageFactory().create("tif", bigImage, bbox)
    val gtw = new GeoTiffWriter(new File("target/density.tif"))
    gtw.write(coverage, params.values().toArray(new Array[GeneralParameterValue](1)))


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
