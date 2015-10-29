package io.hgis.dump

/**
 * Created by willtemperley@gmail.com on 18-Nov-14.
 */

import javax.persistence.EntityManager

import com.esri.core.geometry.OperatorExportToWkb
import com.google.inject.Guice
import com.vividsolutions.jts.io.WKBReader
import io.hgis.ConfigurationFactory
import io.hgis.accessutil.AccessUtil
import io.hgis.domain.EEGrid
import io.hgis.inject.JPAModule
import io.hgis.vector.domain.SiteGridDAO
import org.apache.hadoop.hbase.client._

class DumpEEGrid extends ExtractionBase {

  val CF = "cfv".getBytes

  def geomCol = AccessUtil.geomColumn(jtsWkbReader, CF, "geom") _
  def gridId = AccessUtil.intColumn("cfv", "grid_id") _
  def entityId = AccessUtil.intColumn("cfv", "entity_id") _


  override def getScan: Scan = {
    val scan: Scan = new Scan
    scan.addFamily(CF)
  }


  override def buildEntity(result: Result): Unit = {
    val obj = new EEGrid
    obj.jtsGeom = geomCol(result)
    obj.gridId = gridId(result)
    obj.entityId = entityId(result)
    em.persist(obj)
  }

}

