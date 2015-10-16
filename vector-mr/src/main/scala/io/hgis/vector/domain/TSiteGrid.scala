package io.hgis.vector.domain

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.HasRowKey
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TSiteGrid extends HasRowKey with GriddedEntity {

  var iucnCat: String

  var catId: Int

  var isDesignated: Boolean

  //Very paranoid rowkey generation!
  override def getRowKey: Array[Byte] = {
    getRandomByteArray ++ Bytes.toBytes(entityId) ++ Bytes.toBytes(gridId)
  }
}
