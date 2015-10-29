package io.hgis.load

import io.hgis.ConfigurationFactory
import org.apache.hadoop.hbase.client.HTable

/**
 * Created by willtemperley@gmail.com on 19-Oct-15.
 */
object ExecuteLoad {

  val configuredTables = Map[String, GridLoader[_]](

    "pa_grid" -> new DirectLoadPAs,
    "au_grid" -> new LoadAUs,
    "ee_grid" -> new LoadEEZs,
    "osm_grid" -> new LoadWayGrids,
    "pa" -> new LoadPAs

  )

  //TODO - add truncate option!

  def main(args: Array[String]) {

    if (args.length == 0) {
      println("Please provide a table")
      return
    }
    val arg = args(0)
    val loader = configuredTables.get(arg).get

    val hTable = new HTable(ConfigurationFactory.get, arg)
    loader.executeLoad(hTable)

  }

}
