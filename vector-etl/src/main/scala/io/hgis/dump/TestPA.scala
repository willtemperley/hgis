package io.hgis.dump

/**
 *
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import com.google.inject.Guice
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.WayGrid
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteDAO
import org.apache.hadoop.hbase.client._

class TestPA extends ExtractionBase  {

  val COLFAM: Array[Byte] = "cfv".getBytes

  def gridId = AccessUtil.intColumn(COLFAM, "grid_id") _
  def analysisUnitId = AccessUtil.longColumn(COLFAM, "entity_id") _
  def catId = AccessUtil.intColumn(COLFAM, "cat_id") _

  def getScan: Scan = {
    val scan: Scan = new Scan
    scan
  }

  override def buildEntity(res: Result): Unit = {

    val x = SiteDAO.fromResult(res)

    println("catid: " + x.catId)
    println("desig: " + x.isDesignated)
    println("entityId: " + x.entityId)
    println("geom: " +  x.geom.getType.toString)

  }


}

