package io.hgis.vector.domain

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.{GriddedEntity, HasRowKey}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TSiteGrid extends HasRowKey with GriddedEntity {

  var catId: Int

  var isDesignated: Boolean

  var isPoint: Boolean

  //Very paranoid rowkey generation!
  override def getRowKey: Array[Byte] = {
    getRandomByteArray ++ Bytes.toBytes(entityId) ++ Bytes.toBytes(gridId)
  }
}
