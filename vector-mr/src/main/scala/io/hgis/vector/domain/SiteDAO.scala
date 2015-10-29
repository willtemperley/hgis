package io.hgis.vector.domain

import java.io._
import java.nio.ByteBuffer

import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorImportFromWkb}
import io.hgis.hdomain.SerializableAnalysisUnit
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.WritableUtils

/**
 * Basic structure of PA table in HBase
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object SiteDAO extends SerializableAnalysisUnit[TSite]{

  class Site extends TSite {

    override var geom: Geometry = _
    override var catId: Int = _
    override var gridCells: Array[String] = _
    override var gridIdList: Array[String] = _
    override var entityId: Long = _
    override var jtsGeom: com.vividsolutions.jts.geom.Geometry = _
    override var isDesignated: Boolean = _
    override var isPoint: Boolean = _
  }

  override val getCF: Array[Byte] = "cfv".getBytes

  val GEOM: Array[Byte] = "geom".getBytes
  val GRID_GEOMS: Array[Byte] = "grid".getBytes
  val IS_DESIGNATED: Array[Byte] = "is_designated".getBytes
  val IS_POINT: Array[Byte] = "is_point".getBytes
  val ENTITY_ID: Array[Byte] = "entity_id".getBytes
  val CAT_ID: Array[Byte] = "cat_id".getBytes
  val GRID_ID_LIST: Array[Byte] = "grid_id".getBytes


  val operatorImportFromWkb = OperatorImportFromWkb.local
  val operatorExportToWkb = OperatorExportToWkb.local

  override def toPut(obj: TSite, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)
    put.add(getCF, ENTITY_ID, Bytes.toBytes(obj.entityId))
    put.add(getCF, IS_POINT, Bytes.toBytes(obj.isPoint))
    put.add(getCF, IS_DESIGNATED, Bytes.toBytes(obj.isDesignated))
    put.add(getCF, CAT_ID, Bytes.toBytes(obj.catId))
    put.add(getCF, GRID_GEOMS, serializeStringArray(obj.gridCells))
    put.add(getCF, GRID_ID_LIST, serializeStringArray(obj.gridIdList))
    val bytes: Array[Byte] = operatorExportToWkb.execute(0, obj.geom, null).array()
    put.add(getCF, GEOM, bytes)
    put
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

  override def fromResult(result: Result, site: TSite = new Site): TSite = {

    site.entityId = Bytes.toLong(result.getValue(getCF, ENTITY_ID))
    site.isDesignated = Bytes.toBoolean(result.getValue(getCF, IS_DESIGNATED))
    site.isPoint = Bytes.toBoolean(result.getValue(getCF, IS_POINT))
    val bytes: Array[Byte] = result.getValue(getCF, GEOM)
    site.geom = operatorImportFromWkb.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(bytes), null)
    site.gridCells = deserializeStringArray(result.getValue(getCF, GRID_GEOMS))
    site.gridIdList = deserializeStringArray(result.getValue(getCF, GRID_ID_LIST))
    site.catId = Bytes.toInt(result.getValue(getCF, CAT_ID))

    site
  }
}