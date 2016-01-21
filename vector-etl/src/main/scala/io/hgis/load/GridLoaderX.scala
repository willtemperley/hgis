package io.hgis.load

import org.apache.hadoop.hbase.client.HTableInterface

/**
  * Created by willtemperley@gmail.com on 03-Dec-15.
  */
trait GridLoaderX {

  def executeLoad(table: HTableInterface): Unit

}
