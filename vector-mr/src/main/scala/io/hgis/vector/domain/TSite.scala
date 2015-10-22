package io.hgis.vector.domain

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.{AnalysisUnit, HasRowKey}
import org.apache.hadoop.hbase.util.Bytes


/**
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TSite extends HasRowKey with AnalysisUnit {

  var gridCells: Array[String]

  var gridIdList: Array[String]

  var catId: Int

  var isDesignated: Boolean

  var isPoint: Boolean

  override def getRowKey: Array[Byte] = {
    getRandomByteArray ++ Bytes.toBytes(entityId)
  }

}
