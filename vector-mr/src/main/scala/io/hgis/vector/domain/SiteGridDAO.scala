package io.hgis.vector.domain

import java.nio.ByteBuffer

import com.esri.core.geometry._
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import io.hgis.hdomain.SerializableAnalysisUnit
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes


/**
 * Basic metadata for a PA and grid together
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */
object SiteGridDAO extends SerializableAnalysisUnit[TSiteGrid] {

  class SiteGrid extends TSiteGrid {

    override var gridId: Int = _
    override var geom: Geometry = _
    override var jtsGeom: com.vividsolutions.jts.geom.Geometry = _
    override var iucnCat: String = _
    override var isDesignated: Boolean = _
    override var catId: Int = _
    override var entityId: Long = _
  }

  override def getCF: Array[Byte] = "cfv".getBytes

  val GEOM: Array[Byte] = "geom".getBytes
  val GRID_ID: Array[Byte] = "grid_id".getBytes
  val SITE_ID: Array[Byte] = "site_id".getBytes
  val IUCN_CAT: Array[Byte] = "iucn_cat".getBytes
  val CAT_ID: Array[Byte] = "cat_id".getBytes
  val IS_DESIGNATED: Array[Byte] = "is_designated".getBytes

  def getRowKey(wdpaId: Int, gridId: Int): Array[Byte] = {
    Bytes.toBytes(wdpaId).reverse.++(Bytes.toBytes(gridId).reverse)
  }

  val wktExportOp = OperatorExportToWkt.local()

  val wkbExportOp = OperatorExportToWkb.local()
  val wkbImportOp = OperatorImportFromWkb.local()

  val wkbReader = new WKBReader
  val wkbWriter = new WKBWriter

  override def toPut(obj: TSiteGrid, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)

    put.add(getCF, GRID_ID, Bytes.toBytes(obj.gridId))
    put.add(getCF, CAT_ID, Bytes.toBytes(obj.gridId))
    put.add(getCF, SITE_ID, Bytes.toBytes(obj.entityId))
    put.add(getCF, IUCN_CAT, Bytes.toBytes(obj.iucnCat))
    put.add(getCF, IS_DESIGNATED, Bytes.toBytes(obj.isDesignated))

    val ixPaGrid: Array[Byte] = wkbExportOp.execute(WkbExportFlags.wkbExportDefaults, obj.geom, null).array()
    put.add(getCF, GEOM, ixPaGrid)
  }

  def toPutJTS(obj: TSiteGrid, rowKey: Array[Byte]): Put = {
    val put = new Put(rowKey)

    put.add(getCF, GRID_ID, Bytes.toBytes(obj.gridId))
    put.add(getCF, SITE_ID, Bytes.toBytes(obj.entityId))
    put.add(getCF, IUCN_CAT, Bytes.toBytes(obj.iucnCat))
    put.add(getCF, IS_DESIGNATED, Bytes.toBytes(obj.isDesignated))

  }

  override def fromResult(result: Result, siteGrid: TSiteGrid = new SiteGrid): TSiteGrid = {

    siteGrid.gridId = Bytes.toInt(result.getValue(getCF, GRID_ID))
    siteGrid.entityId = Bytes.toInt(result.getValue(getCF, SITE_ID))

    val v = result.getValue(getCF, CAT_ID)
    if (v != null) {
      siteGrid.catId = Bytes.toInt(v)
    }

    siteGrid.isDesignated = Bytes.toBoolean(result.getValue(getCF, IS_DESIGNATED))

    siteGrid.geom = wkbImportOp.execute(0, Geometry.Type.Polygon, ByteBuffer.wrap(result.getValue(getCF, GEOM)), null)
//    griddedEntity.jtsGeom = wkbReader.read(result.getValue(getCF, GEOM))

    siteGrid
  }


}
