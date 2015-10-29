package io.hgis.dump

/**
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import com.google.inject.Guice
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.{WayGrid, EEPro, EcoregionEEZProtection}
import io.hgis.inject.JPAModule
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes

class DumpWayGrid extends ExtractionBase  {

  val CF: Array[Byte] = "cfv".getBytes

  def gridId = AccessUtil.intColumn(CF, "grid_id") _
  def analysisUnitId = AccessUtil.longColumn(CF, "entity_id") _
  def catId = AccessUtil.intColumn(CF, "cat_id") _

  def getScan: Scan = {
    val scan: Scan = new Scan
    scan
  }

  override def buildEntity(res: Result): Unit = {

    val x = new WayGrid
    x.jtsGeom = jtsWkbReader.read(res.getValue("cfv".getBytes, "geom".getBytes))
    x.gridId = gridId(res)
//    x.entityId = analysisUnitId(res)

    em.persist(x)

  }


}

