package io.hgis.vector.domain

import java.nio.ByteBuffer

import com.esri.core.geometry
import com.esri.core.geometry.{OperatorExportToWkb, OperatorImportFromWkb, WkbExportFlags, _}
import io.hgis.hdomain.HSerializable
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Basic structure of PA table in HBase
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object SiteAnalysisUnitDAO extends HSerializable[TAnalysisUnitSite]{

  class AnalysisUnitSite extends TAnalysisUnitSite {
    override var siteId: Int = _
    override var geom: geometry.Geometry = _
    override var entityId: Int = _
    override var jtsGeom: com.vividsolutions.jts.geom.Geometry = _
  }

  override def getCF: Array[Byte] = "cfv".getBytes

  val SITE_ID: Array[Byte] = "site_id".getBytes
  val GRID_ID: Array[Byte] = "grid_id".getBytes
  val GEOM: Array[Byte] = "geom".getBytes

  val wkbImportOp = OperatorImportFromWkb.local
  val wkbExportOp = OperatorExportToWkb.local

  override def toPut(obj: TAnalysisUnitSite, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)
    put.add(getCF, GRID_ID, Bytes.toBytes(obj.entityId))
    put.add(getCF, SITE_ID, Bytes.toBytes(obj.siteId))
    val geomBytes: Array[Byte] = wkbExportOp.execute(WkbExportFlags.wkbExportDefaults, obj.geom, null).array()
    put.add(getCF, GEOM, geomBytes)
    put
  }


  override def fromResult(result: Result, domainObj: TAnalysisUnitSite = new AnalysisUnitSite): TAnalysisUnitSite = {

    domainObj.entityId = Bytes.toInt(result.getValue(getCF, GRID_ID))
    domainObj.siteId = Bytes.toInt(result.getValue(getCF, SITE_ID))
    domainObj.geom = wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(result.getValue(getCF, GEOM)), null)
    domainObj

  }
}
