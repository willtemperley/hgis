package io.hgis.vector.domain

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorImportFromWkb}
import com.vividsolutions.jts.geom
import io.hgis.accessutil.AccessUtil
import io.hgis.hdomain.SerializableAnalysisUnit
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.WritableUtils

/**
 * For loading countries etc
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object AdminUnitDAO extends SerializableAnalysisUnit[TAdminUnit]{

  class AdminUnit extends TAdminUnit {

    override var entityId: Long = _
    override var geom: Geometry = _

    override var jtsGeom: com.vividsolutions.jts.geom.Geometry = _
  }

  override val getCF: Array[Byte] = "cfv".getBytes

  val GEOM: Array[Byte] = "geom".getBytes
  val GRID_GEOMS: Array[Byte] = "grid".getBytes
  val ENTITY_ID: Array[Byte] = "entity_id".getBytes
  val GRID_ID_LIST: Array[Byte] = "grid_id".getBytes


  val operatorImportFromWkb = OperatorImportFromWkb.local
  val operatorExportToWkb = OperatorExportToWkb.local

  override def toPut(obj: TAdminUnit, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)
    put.add(getCF, ENTITY_ID, Bytes.toBytes(obj.entityId))
    val bytes: Array[Byte] = operatorExportToWkb.execute(0, obj.geom, null).array()
    put.add(getCF, GEOM, bytes)
    put
  }


  override def fromResult(result: Result, site: TAdminUnit = new AdminUnit): TAdminUnit = {

    site.entityId= Bytes.toInt(result.getValue(getCF, ENTITY_ID))
    val bytes: Array[Byte] = result.getValue(getCF, GEOM)
    site.geom = operatorImportFromWkb.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(bytes), null)

    //TODO: Remove return value

    site
  }
}
