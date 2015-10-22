package io.hgis.osmdomain

import com.vividsolutions.jts.geom.Geometry
import io.hgis.hdomain.{AnalysisUnit, HasRowKey}
import org.apache.hadoop.hbase.util.Bytes


/**
 *
 * Created by willtemperley@gmail.com on 19-Nov-14.
 */
trait TWay extends HasRowKey with AnalysisUnit {

  override def getRowKey: Array[Byte] = {
    getRandomByteArray ++ Bytes.toBytes(entityId)
  }

}
