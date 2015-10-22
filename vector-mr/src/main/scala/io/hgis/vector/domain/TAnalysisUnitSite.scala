package io.hgis.vector.domain

import com.esri.core.geometry.Geometry
import io.hgis.hdomain.{AnalysisUnit, HasRowKey}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TAnalysisUnitSite extends HasRowKey with AnalysisUnit {

  var siteId: Int

  //Very paranoid rowkey generation!
  override def getRowKey: Array[Byte] = {
    getRandomByteArray ++ Bytes.toBytes(siteId) ++ Bytes.toBytes(entityId)
  }
}
