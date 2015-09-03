package io.hgis.dump

import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.hgrid.GlobalGrid
import org.apache.hadoop.hbase.client.{HTable, Scan}

/**
 * Created by willtemperley@gmail.com on 01-Jun-15.
 *
 * This is getting better; using a better interator syntax and a more generic way of extracting geometries.
 *
 */
// FIXME make generic
object DumpPointDensity extends GeometryScanner {

  val grid = new GlobalGrid(4320, 2160, 1024)
  val wkbReader = new WKBReader

  def countCol = AccessUtil.intColumn("cfv", "count") _
  def geomCol = AccessUtil.geomColumn(wkbReader, "cfv", "geom") _

  def main(args: Array[String]) {

    val sw = new ShapeWriter()
    val htable = new HTable(ConfigurationFactory.get, "hashcount")
//    System.out.println("Got Htable " + htable)

    val scan = new Scan
    scan.addFamily("cfv".getBytes)
    val scanner = htable.getScanner(scan)

    val ways = getIterator(scanner)

    val vals = new Array[Int](4320 * 2160)
    for ( way <- ways ) {

      val geom = geomCol(way).asInstanceOf[Point]
      val count = countCol(way)

//      val(x, y) = grid.snap(geom.getX, geom.getY)

      sw.addFeature(geom, Seq(count, 0))
//      val idx: Int = grid.toRasterIdx(x, y)
//      try {
//        vals(idx) = count
//      } catch {
//        case e: Exception =>
//          println("x="+x)
//          println("y="+y)
//          println("idx="+idx)
//          e.printStackTrace()
//      }

    }

//    RasterWriter.paint(4320, 2160, vals, new File("target/ras.png"))

    sw.write("target/pts.shp")
  }


}
