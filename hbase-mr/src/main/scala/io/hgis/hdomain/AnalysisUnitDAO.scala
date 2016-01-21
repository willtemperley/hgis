package io.hgis.hdomain

import java.nio.ByteBuffer

import com.esri.core.geometry._
import com.vividsolutions.jts.geom
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object AnalysisUnitDAO extends SerializableAnalysisUnit[AnalysisUnit] with ConvertsGeometry {

  class AnalsysisUnitImpl extends AnalysisUnit {
    override var entityId: Long = _
    override var geom: Geometry = _
    override var jtsGeom: com.vividsolutions.jts.geom.Geometry = _
  }

  override def getCF: Array[Byte] = "cfv".getBytes

  val GEOM: Array[Byte] = "geom".getBytes
  val ENTITY_ID: Array[Byte] = "entity_id".getBytes

  def getRowKey(wdpaId: Int, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }


  override def toPut(obj: AnalysisUnit, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)

    put.add(getCF, ENTITY_ID, Bytes.toBytes(obj.entityId))
    val geom: Array[Byte] = esriWkbWriter.execute(WkbExportFlags.wkbExportDefaults, obj.geom, null).array()
    put.add(getCF, GEOM, geom)
  }

  override def fromResult(result: Result, griddedEntity: AnalysisUnit = new AnalsysisUnitImpl): AnalysisUnit = {

    griddedEntity.entityId = Bytes.toLong(result.getValue(getCF, ENTITY_ID))
    griddedEntity.geom = esriWkbReader.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(result.getValue(getCF, GEOM)), null)

    griddedEntity
  }


}
