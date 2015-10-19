package io.hgis.load

import io.hgis.ConfigurationFactory
import org.apache.hadoop.hbase.client.HTable

/**
 * Created by willtemperley@gmail.com on 19-Oct-15.
 */
object ExecuteLoad {

  def main(args: Array[String]) {

    val hTable = new HTable(ConfigurationFactory.get, "pa_grid")
    val paL = new LoadPAs
    paL.executeLoad(hTable)

  }

}
