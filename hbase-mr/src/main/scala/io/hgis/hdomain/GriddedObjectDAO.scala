package io.hgis.hdomain

import java.nio.ByteBuffer

import com.esri.core.geometry._
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object GriddedObjectDAO extends SerializableAnalysisUnit[GriddedEntity] with ConvertsGeometry {

  class GriddedObject extends GriddedEntity {

    override var geom: Geometry = _
    override var entityId: Long = _
    override var jtsGeom: com.vividsolutions.jts.geom.Geometry = _
    override var gridId: Int = _
  }

  override def getCF: Array[Byte] = "cfv".getBytes

  val GEOM: Array[Byte] = "geom".getBytes
  val GRID_ID: Array[Byte] = "grid_id".getBytes
  val ENTITY_ID: Array[Byte] = "entity_id".getBytes

  def getRowKey(wdpaId: Int, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }


  override def toPut(obj: GriddedEntity, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)

    if (obj.entityId == 0) throw new RuntimeException("entity_id is zero")

    put.add(getCF, GRID_ID, Bytes.toBytes(obj.gridId))
    put.add(getCF, ENTITY_ID, Bytes.toBytes(obj.entityId))
    val geom: Array[Byte] = esriWkbWriter.execute(WkbExportFlags.wkbExportDefaults, obj.geom, null).array()
    put.add(getCF, GEOM, geom)
  }

  override def fromResult(result: Result, griddedEntity: GriddedEntity = new GriddedObject): GriddedEntity = {

    griddedEntity.gridId = Bytes.toInt(result.getValue(getCF, GRID_ID))
    griddedEntity.entityId = Bytes.toInt(result.getValue(getCF, ENTITY_ID))

    griddedEntity.geom = esriWkbReader.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(result.getValue(getCF, GEOM)), null)

    griddedEntity
  }


}
