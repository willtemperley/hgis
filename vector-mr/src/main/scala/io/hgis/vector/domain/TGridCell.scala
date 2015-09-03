package io.hgis.vector.domain

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.HasRowKey
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TGridCell extends HasRowKey {

  var geom: Geometry

  var gridId: Int

  override def getRowKey: Array[Byte] = {
    Bytes.toBytes(gridId)
  }

}
