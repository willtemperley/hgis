package io.hgis.vector.domain

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorImportFromWkb}
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

    override var siteId: Int = _
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
    put.add(getCF, SITE_ID, Bytes.toBytes(obj.siteId))
    put.add(getCF, GRID_GEOMS, serializeStringArray(obj.gridCells))
    put.add(getCF, GRID_ID_LIST, serializeStringArray(obj.gridIdList))
    val bytes: Array[Byte] = operatorExportToWkb.execute(0, obj.geom, null).array()
    put.add(getCF, GEOM, bytes)
  }

  def serializeStringArray(strings: Array[String]): Array[Byte] = {
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val dOut = new DataOutputStream(byteArrayOutputStream)
    WritableUtils.writeStringArray(dOut, strings.toArray)
    val encoded = byteArrayOutputStream.toByteArray
    encoded
  }

  def deserializeStringArray(bytes: Array[Byte]): Array[String] = {
    val bis = new ByteArrayInputStream(bytes)
    val inputStream: DataInputStream = new DataInputStream(bis)
    val strings = WritableUtils.readStringArray(inputStream)
    strings
  }

  override def fromResult(result: Result, site: TAdminUnit = new AdminUnit): TAdminUnit = {

    site.siteId = Bytes.toInt(result.getValue(getCF, SITE_ID))
    val bytes: Array[Byte] = result.getValue(getCF, GEOM)
    site.geom = operatorImportFromWkb.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(bytes), null)
    site.gridCells = deserializeStringArray(result.getValue(getCF, GRID_GEOMS))
    site.gridIdList = deserializeStringArray(result.getValue(getCF, GRID_ID_LIST))

    //TODO: Remove return value

    site
  }
}
