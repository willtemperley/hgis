package io.hgis.load

import java.util.concurrent.TimeUnit

import io.hgis.ConfigurationFactory
import org.apache.hadoop.hbase.client.HTable

/**
 * Created by willtemperley@gmail.com on 19-Oct-15.
 */
object ExecuteLoad {

  val configuredTables = Map[String, GridLoader[_]](

//    "sp_grid" -> new LoadSppContextSensitive,
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

    val t0 = System.currentTimeMillis()

    val hTable = new HTable(ConfigurationFactory.get, arg)
    loader.executeLoad(hTable)

    printElapsedTime(t0)

  }

  def printElapsedTime(t0: Long): Unit = {
    var millis = System.currentTimeMillis() - t0
    val hour = TimeUnit.MILLISECONDS.toHours(millis)
    val minute = TimeUnit.MILLISECONDS.toMinutes(millis)
    val second = TimeUnit.MILLISECONDS.toSeconds(millis)
    millis -= TimeUnit.SECONDS.toMillis(second)
    println(f"$hour%02d:$minute%02d:$second%02d:$millis")
  }
}
