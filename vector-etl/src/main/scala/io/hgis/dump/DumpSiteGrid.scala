package io.hgis.dump

/**
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import io.hgis.accessutil.AccessUtil
import io.hgis.domain.{SiteGrid, WayGrid}
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client._

class DumpSiteGrid extends ExtractionBase  {

  val CF: Array[Byte] = "cfv".getBytes

//  def gridId = AccessUtil.intColumn(CF, "grid_id") _
//  def analysisUnitId = AccessUtil.longColumn(CF, "entity_id") _
//  def catId = AccessUtil.intColumn(CF, "cat_id") _

  def getScan: Scan = {
    val scan = new Scan
    scan.addFamily(CF)
  }

  override def buildEntity(res: Result): Unit = {

    val sg = SiteGridDAO.fromResult(res, new SiteGrid)
    sg.jtsGeom = jtsWkbReader.read(res.getValue(CF, "geom".getBytes))
//    x.gridId = gridId(res)

//    x.entityId = analysisUnitId(res)

    em.persist(sg)

  }


}

