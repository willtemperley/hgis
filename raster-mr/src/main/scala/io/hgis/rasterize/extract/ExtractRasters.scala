package io.hgis.rasterize.extract

import java.io.{File, FileOutputStream, PrintWriter}

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKTReader
import io.hgis.ConfigurationFactory
import io.hgis.hgrid.GlobalGrid
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by willtemperley@gmail.com on 05-Jun-15.
 *
 */
object ExtractRasters extends TableIterator {

  val wktReader = new WKTReader()
  val grid = new GlobalGrid(43200, 21600, 1080)

  def main(args: Array[String]): Unit = {

    val htable = new HTable(ConfigurationFactory.get, "osm_tile")

    val scan = new Scan
    scan.addFamily("cfv".getBytes)
    val scanner = htable.getScanner(scan)
    val ways = getIterator(scanner)

    for (img <- ways) {

      val value = img.getValue("cfv".getBytes, "error".getBytes)
      if (value != null) {
        println(Bytes.toString(value))
      } else {

        val v = img.getValue("cfv".getBytes, "image".getBytes)

        //      val p = img.getValue("cfv".getBytes, "origin".getBytes)


        val orig = grid.gridIdToOrigin(img.getRow)
        val name = orig._1 + "_" + orig._2

        val fos = new FileOutputStream(new File("target/" + name + ".png"))
        fos.write(v)

        writePNGWfile(name,  grid.pixelToPoint(orig._1, orig._2))
      }

    }

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
