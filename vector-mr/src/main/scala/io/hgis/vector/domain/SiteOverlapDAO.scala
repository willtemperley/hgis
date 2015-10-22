package io.hgis.vector.domain

import com.esri.core.geometry
import com.esri.core.geometry.{OperatorExportToWkb, OperatorImportFromWkb}
import com.vividsolutions.jts.geom.Geometry
import io.hgis.hdomain.SerializableAnalysisUnit
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Basic structure of PA table in HBase
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object SiteOverlapDAO extends SerializableAnalysisUnit[TSiteOverlap]{

  class SiteOverlap extends TSiteOverlap {

    override var siteId1: Long = _
    override var siteId2: Long = _
    override var area: Double = _
    override var entityId: Long = _
    override var geom: geometry.Geometry = _
    override var jtsGeom: Geometry = _
  }

  override def getCF: Array[Byte] = "cfv".getBytes

  val SITE_ID1: Array[Byte] = "site_id1".getBytes
  val SITE_ID2: Array[Byte] = "site_id2".getBytes
  val AREA: Array[Byte] = "area".getBytes

  val operatorImportFromWkb = OperatorImportFromWkb.local
  val operatorExportToWkb = OperatorExportToWkb.local

  override def toPut(obj: TSiteOverlap, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)
    put.add(getCF, SITE_ID1, Bytes.toBytes(obj.siteId1))
    put.add(getCF, SITE_ID2, Bytes.toBytes(obj.siteId2))
    put.add(getCF, AREA, Bytes.toBytes(obj.area))
    put
  }


  override def fromResult(result: Result, domainObj: TSiteOverlap = new SiteOverlap): TSiteOverlap = {

    domainObj.siteId1 = Bytes.toInt(result.getValue(getCF, SITE_ID1))
    domainObj.siteId2 = Bytes.toInt(result.getValue(getCF, SITE_ID2))
    domainObj.area = Bytes.toDouble(result.getValue(getCF, AREA))
    domainObj

  }
}
