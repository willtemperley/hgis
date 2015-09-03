package io.hgis.vector.domain

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.HasRowKey
import org.apache.hadoop.hbase.util.Bytes


/**
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TSite extends HasRowKey {

  var geom: Geometry
  var isDesignated: Boolean;
  var siteId: Int
  var name: String
  var iucnCat: String

  var gridCells: Array[String]
  var gridIdList: Array[String]

  override def getRowKey: Array[Byte] = {
    getRandomByteArray ++ Bytes.toBytes(siteId)
  }

}
