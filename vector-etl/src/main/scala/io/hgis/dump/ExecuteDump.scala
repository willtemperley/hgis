package io.hgis.dump

import io.hgis.ConfigurationFactory
import io.hgis.load._
import org.apache.hadoop.hbase.client.HTable

/**
 * Created by willtemperley@gmail.com on 19-Oct-15.
 */
object ExecuteDump {

  val configuredTables = Map[String, ExtractionBase[_]](

    "ee_protection" -> new DumpEcoregionProtection,
    "osm_grid" -> new DumpWayGrid

  )


  def main(args: Array[String]) {

    if (args.length == 0) {
      println("Please provide a table")
      return
    }
    val arg = args(0)
    val dumper = configuredTables.get(arg).get

    val hTable = new HTable(ConfigurationFactory.get, arg)

    dumper.executeExtract(hTable)

  }

}
