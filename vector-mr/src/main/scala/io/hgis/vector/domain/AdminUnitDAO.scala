package io.hgis.vector.domain

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorImportFromWkb}
import io.hgis.accessutil.AccessUtil
import io.hgis.hdomain.HSerializable
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.WritableUtils

/**
 * For loading countries etc
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object AdminUnitDAO extends HSerializable[TAdminUnit]{

  class AdminUnit extends TAdminUnit {

    override var analysisUnitId: Int = _
    override var geom: Geometry = _

    override var gridCells: Array[String] = _
    override var gridIdList: Array[String] = _

  }

  override val getCF: Array[Byte] = "cfv".getBytes

  val GEOM: Array[Byte] = "geom".getBytes
  val GRID_GEOMS: Array[Byte] = "grid".getBytes
  val SITE_ID: Array[Byte] = "site_id".getBytes
  val GRID_ID_LIST: Array[Byte] = "grid_id".getBytes


  val operatorImportFromWkb = OperatorImportFromWkb.local
  val operatorExportToWkb = OperatorExportToWkb.local

  override def toPut(obj: TAdminUnit, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)
    put.add(getCF, SITE_ID, Bytes.toBytes(obj.analysisUnitId))
    put.add(getCF, GRID_GEOMS, AccessUtil.serializeStringArray(obj.gridCells))
    put.add(getCF, GRID_ID_LIST, AccessUtil.serializeStringArray(obj.gridIdList))
    val bytes: Array[Byte] = operatorExportToWkb.execute(0, obj.geom, null).array()
    put.add(getCF, GEOM, bytes)
  }


  override def fromResult(result: Result, site: TAdminUnit = new AdminUnit): TAdminUnit = {

    site.analysisUnitId = Bytes.toInt(result.getValue(getCF, SITE_ID))
    val bytes: Array[Byte] = result.getValue(getCF, GEOM)
    site.geom = operatorImportFromWkb.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(bytes), null)
    site.gridCells = AccessUtil.deserializeStringArray(result.getValue(getCF, GRID_GEOMS))
    site.gridIdList = AccessUtil.deserializeStringArray(result.getValue(getCF, GRID_ID_LIST))

    //TODO: Remove return value

    site
  }
}
