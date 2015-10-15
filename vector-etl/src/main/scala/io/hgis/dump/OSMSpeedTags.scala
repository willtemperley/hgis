package io.hgis.dump

import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.scanutil.TableIterator
import org.apache.hadoop.hbase.client.{HTable, Scan}

/**
 * Created by willtemperley@gmail.com on 05-Jun-15.
 *
 */
object OSMSpeedTags extends TableIterator {

  val CFV = "cfv".getBytes

  val highwayColumn  = AccessUtil.stringColumn(CFV, "highway") _
  val speedColumn    = AccessUtil.stringColumn(CFV, "maxspeed") _
  val countryColumn  = AccessUtil.stringColumn(CFV, "country") _
  val countColumn    = AccessUtil.intColumn("cfv", "count") _

  def main(args: Array[String]): Unit = {

    val htable = new HTable(ConfigurationFactory.get, "osm_speed_tags")

    val scan = new Scan
    scan.addFamily(CFV)
    scan.addColumn(CFV, "highway".getBytes)
    scan.addColumn(CFV, "maxspeed".getBytes)
    scan.addColumn(CFV, "country".getBytes)
    scan.addColumn(CFV, "count".getBytes)

    val scanner = htable.getScanner(scan)
    val ways = getIterator(scanner)

    for (rec <- ways) {
      val out = Array(countryColumn(rec), highwayColumn(rec), speedColumn(rec), countColumn(rec)).mkString("\t")
      println(out)
    }

  }
}
