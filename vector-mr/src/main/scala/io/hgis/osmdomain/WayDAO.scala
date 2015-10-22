package io.hgis.osmdomain

import com.esri.core.geometry
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import io.hgis.hdomain.SerializableAnalysisUnit
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * Created by tempehu on 23-Apr-15.
 */
object WayDAO extends SerializableAnalysisUnit[TWay] {

  class Way extends TWay {

    override var entityId: Long = _
    override var geom: geometry.Geometry = _
    override var jtsGeom: Geometry = _
  }
  
  override def toPut(obj: TWay, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)

    put.add(getCF, ID, Bytes.toBytes(obj.entityId))
    put.add(getCF, GEOM, wkbWriter.write(obj.jtsGeom))
//    val bytes: Array[Byte] = operatorExportToWkb.execute(0, obj.geom, null).array()
  }

  /**
   * The column family this domain object uses
   *
   * @return
   */
  override def getCF: Array[Byte] = {
    "cfv".getBytes
  }


  val wkbWriter = new WKBWriter
  val wkbReader = new WKBReader

  val VERSION: Array[Byte] = "version".getBytes
  val USER_ID: Array[Byte] = "userId".getBytes
  val TSTAMP: Array[Byte] = "tStamp".getBytes
  val HEX_GEOM: Array[Byte] = "hexGeom".getBytes
  val DATE_UPDATED: Array[Byte] = "date_updated".getBytes
  val TAGS: Array[Byte] = "tags".getBytes
  val CHANGESET_ID: Array[Byte] = "changesetId".getBytes
  val ID: Array[Byte] = "id".getBytes
  val GEOM = "geom".getBytes

  /**
   * Decodes a result into a domain object
   *
   * @param result An HBase row result
   * @return A domain object of type T
   */
  override def fromResult(result: Result, way: TWay = new Way): TWay = {
    way.entityId = Bytes.toInt(result.getValue(getCF, ID))
    way.jtsGeom = wkbReader.read(result.getValue(getCF, GEOM))
    way
  }
}
