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

class DumpWayGrid extends ExtractionBase[WayGrid]  {

  val COLFAM: Array[Byte] = "cfv".getBytes

  val injector = Guice.createInjector(new JPAModule)
  
// = new HTable(ConfigurationFactory.get, "ee_protection")
  val catID = 1

  def gridId = AccessUtil.intColumn(COLFAM, "grid_id") _
  def analysisUnitId = AccessUtil.longColumn(COLFAM, "entity_id") _
  def catId = AccessUtil.intColumn(COLFAM, "cat_id") _

  def getScan: Scan = {
    val scan: Scan = new Scan
    scan
  }

  override def persistEntity(res: Result, x: WayGrid): Unit = {

    x.jtsGeom = jtsWkbReader.read(res.getValue("cfv".getBytes, "geom".getBytes))
    x.gridId = gridId(res)
//    x.entityId = analysisUnitId(res)

    em.persist(x)

  }

  override def createEntity: WayGrid = new WayGrid

}

