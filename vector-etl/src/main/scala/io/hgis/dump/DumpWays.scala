package io.hgis.dump

import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.osmdomain.WayDAO
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{Scan, HTable}

/**
 * Created by willtemperley@gmail.com on 05-Jun-15.
 *
 */
object DumpWays extends TableIterator {

  val wkbReader = new WKBReader()

  def getGeom = AccessUtil.geomColumn(wkbReader, "cfv", "geom") _

  def main(args: Array[String]): Unit = {

    val htable = new HTable(ConfigurationFactory.get, "extracted_ways")

    val scan = new Scan
    scan.addFamily("cfv".getBytes)
    scan.addColumn("cfv".getBytes, "geom".getBytes)
    scan.addColumn("cfv".getBytes, "id".getBytes)

    val scanner = htable.getScanner(scan)

    val ways = getIterator(scanner)


    for (r <- ways) {
      val w = WayDAO.fromResult(r)
      println(w.entityId)
    }

//    val sw = new ShapeWriter("LineString", 4326)

//    ways.foreach(f => sw.addFeature(getGeom(f), Seq(0,0)))
//
//    sw.write("target/extracted_ways.shp")


  }
}
